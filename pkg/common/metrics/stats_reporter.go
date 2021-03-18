/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"context"
	"fmt"
	"regexp"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	"knative.dev/pkg/metrics"
)

// StatsReporter defines the interface for sending ingress metrics.
type StatsReporter interface {
	Report(ReportingList)
}

// Verify StatsReporter Implements StatsReporter Interface
var _ StatsReporter = &Reporter{}

// Regular expressions for extracting the particular broker and topic from Sarama metric identifiers
var regexSpecificBroker = regexp.MustCompile(`^.*-for-broker-(.*)`)
var regexSpecificTopic = regexp.MustCompile(`^.*-for-topic-(.*)`)

// The broker and topic keys are constant but need to be created via a function, so they are defined here
var keyBroker = tag.MustNewKey("broker")
var keyTopic = tag.MustNewKey("topic")

// Define a list of regular expressions that can be applied to a raw incoming Sarama metric in order to produce
// text suitable for the "# HELP" section of the metrics endpoint output
// Note that all of the expressions are executed in-order, so a string such as "request-rate-for-broker-0" will
// first replace "request-rate" with "Requests/second sent to all brokers" and then also replace
// "all brokers-for-broker-0" with " for broker 0" to produce the final description.
var regexDescriptions = []struct {
	Search  *regexp.Regexp
	Replace string
}{
	// Sarama Histograms
	{regexp.MustCompile(`^request-size`), `Distribution of the request size in bytes for all brokers`},
	{regexp.MustCompile(`^request-latency-in-ms`), `Distribution of the request latency in ms for all brokers`},
	{regexp.MustCompile(`^response-size`), `Distribution of the response size in bytes for all brokers`},
	{regexp.MustCompile(`^batch-size`), `Distribution of the number of bytes sent per partition per request for all topics`},
	{regexp.MustCompile(`^records-per-request`), `Distribution of the number of records sent per request for all topics`},
	{regexp.MustCompile(`^compression-ratio`), `Distribution of the compression ratio times 100 of record batches for all topics`},
	{regexp.MustCompile(`^consumer-batch-size`), `Distribution of the number of messages in a batch`},

	// Sarama Meters
	{regexp.MustCompile(`^incoming-byte-rate`), `Bytes/second read of all brokers`},
	{regexp.MustCompile(`^request-rate`), `Requests/second sent to all brokers`},
	{regexp.MustCompile(`^response-rate`), `Responses/second received from all brokers`},
	{regexp.MustCompile(`^record-send-rate`), `Records/second sent to all topics`},
	{regexp.MustCompile(`^outgoing-byte-rate`), `Bytes/second written of all brokers`},

	// Sarama Counters
	{regexp.MustCompile(`^requests-in-flight`), `The current number of in-flight requests awaiting a response for all brokers`},

	// Touch-ups for specific topics/brokers
	{regexp.MustCompile(`all topics-for-topic-(.*)`), `topic "${1}"`},
	{regexp.MustCompile(`all brokers-for-broker-`), `broker `},
}

// The saramaMetricInfo struct holds information related to a particular Sarama metric for use by the Views and Tags
type saramaMetricInfo struct {
	Name string
	Description string
	Broker string
	Topic string
}

// Since regular expressions are somewhat costly and the metrics are repetitive, this cache will hold a simple
// string-to-saramaMetricInfo direct replacement
var replacementCache = map[string]saramaMetricInfo{}

// Define StatsReporter Structure
type Reporter struct {
	views        map[string]*view.View
	tagKeys      map[string]tag.Key
	tagContexts  map[string]context.Context
	logger       *zap.Logger
}

// StatsReporter Constructor
func NewStatsReporter(log *zap.Logger) StatsReporter {
	return &Reporter{
		views:        make(map[string]*view.View),
		logger:       log,
		tagContexts:  make(map[string]context.Context),
	}
}

// Our RecordWrapper, which defaults to the knative metrics.Record()
// This wrapper function facilitates minimally-invasive unit testing of the
// Report functionality without requiring live servers to be started.
var RecordWrapper = metrics.Record

// Some type aliases for the otherwise unwieldy metric collection map-of-maps-to-interfaces
type ReportingItem = map[string]interface{}
type ReportingList = map[string]ReportingItem

//
// Report The Sarama Metrics (go-metrics) Via Knative / OpenCensus Metrics
//
func (r *Reporter) Report(list ReportingList) {

	// Validate The Metrics
	if len(list) > 0 {

		// Loop Over The Observed Metrics
		for metricKey, metricValue := range list {

			// Record Each Individual Metric Item
			for saramaKey, saramaValue := range metricValue {
				r.recordMeasurement(metricKey, saramaKey, saramaValue)
			}
		}
	}
}

// Creates and registers a new view in the OpenCensus context, adding it to the Reporter's known views
// Note:  OpenCensus has a "view.Distribution" that can be used for the Aggregation field, but the Sarama
//        measurements (1-Minute Rate, 5-Minute Rate, etc.) are already in a collective form, so the only way
//        to record them via OpenCensus is to use the individual measurements and offer them to the end-user
//        as individual views for whatever purpose they desire.  This isn't perfect and it may be better in
//        the future to switch to something other than the default OpenCensus recorder.
func (r *Reporter) createView(info saramaMetricInfo, measure stats.Measure) (*view.View, error) {
	newView := &view.View{
		Name:        info.Name,
		Description: info.Description,
		Measure:     measure,
		TagKeys:     []tag.Key{keyBroker, keyTopic},
		Aggregation: view.LastValue(), // Sarama already sums or otherwise aggregates its metrics, so only LastValue is useful here
	}
	err := view.Register(newView)
	if err != nil {
		return nil, err
	}

	r.tagContexts[info.Name] = context.Background()
	ctx, err := tag.New(r.tagContexts[info.Name], tag.Insert(keyBroker, info.Broker))
	if err != nil {
		return nil, err
	}
	r.tagContexts[info.Name] = ctx

	ctx, err = tag.New(ctx, tag.Insert(keyTopic, info.Topic))
	if err != nil {
		return nil, err
	}
	r.tagContexts[info.Name] = ctx


	//r.logger.Info(fmt.Sprintf("\nEDV: View Name: %v\nView Description: %v\nMeasure Name: %v\nMeasure Description: %v\nTag Keys: %v\n", newView.Name, newView.Description, newView.Measure.Name(), newView.Measure.Description(), newView.TagKeys))
	r.views[info.Name] = newView
	return newView, nil
}

// Record a measurement to the metrics backend, creating a new OpenCensus view if this is a new measurement
func (r *Reporter) recordMeasurement(metricKey string, saramaKey string, value interface{}) {

	// Type-switches don't support "fallthrough" so each individual possible type must have its own
	// somewhat-redundant code block.  Not all types are used by Sarama at the moment; if a new type is
	// added, a warning will be logged here.
	// Note:  The Int64Measure wrapper converts to a float64 internally anyway so there is no particular
	//        advantage in treating int-types separately here.
	switch value := value.(type) {
	case int:
		r.recordFloat(float64(value), metricKey, saramaKey)
	case int32:
		r.recordFloat(float64(value), metricKey, saramaKey)
	case int64:
		r.recordFloat(float64(value), metricKey, saramaKey)
	case float64:
		r.recordFloat(value, metricKey, saramaKey)
	case float32:
		r.recordFloat(float64(value), metricKey, saramaKey)
	default:
		r.logger.Warn("Could not interpret Sarama measurement as a number", zap.Any("Sarama Value", value))
	}
}

// Record a measurement that is represented by a float64 value
func (r *Reporter) recordFloat(value float64, metricKey string, saramaKey string) {
	measureInfo := getMetricSubInfo(metricKey, saramaKey)

	var err error
	floatView, ok := r.views[measureInfo.Name]
	if !ok {
		floatView, err = r.createView(measureInfo, stats.Float64(measureInfo.Name, measureInfo.Description, stats.UnitDimensionless))
		if err != nil {
			r.logger.Error("Failed to register OpenCensus views", zap.Error(err))
			return
		}
	}
	RecordWrapper(r.tagContexts[measureInfo.Name], floatView.Measure.(*stats.Float64Measure).M(value))
}

// Returns pretty descriptions for known Sarama metrics
func getMetricInfo(metricKey string) saramaMetricInfo {
	if cachedReplacement, ok := replacementCache[metricKey]; ok {
		return cachedReplacement
	}
	newString := metricKey
	for _, replacement := range regexDescriptions {
		newString = replacement.Search.ReplaceAllString(newString, replacement.Replace)
	}

	broker := "all"
	if regexSpecificBroker.MatchString(metricKey) {
		broker = regexSpecificBroker.ReplaceAllString(metricKey, "${1}")
	}
	topic := "any"
	if regexSpecificTopic.MatchString(metricKey) {
		topic = regexSpecificTopic.ReplaceAllString(metricKey, "${1}")
	}

	info := saramaMetricInfo{
		Name:        metricKey,
		Description: newString,
		Broker:      broker,
		Topic:       topic,
	}
	replacementCache[metricKey] = info
	return info
}

// Returns pretty descriptions for known Sarama submetrics
func getMetricSubInfo(main string, sub string) saramaMetricInfo {
	if cachedReplacement, ok := replacementCache[main+sub]; ok {
		return cachedReplacement
	}
	// Run through the list of known replacements that should be made (multiple replacements may happen)
	info := getMetricInfo(main)
	info.Name = fmt.Sprintf("%s.%s", main, sub)
	info.Description += ": " + getSubDescription(sub)
	replacementCache[main+sub] = info
	return info
}

// Returns pretty descriptions for known Sarama sub-metric categories
func getSubDescription(sub string) string {
	switch sub {
	case "1m.rate":
		return "1-Minute Rate"
	case "5m.rate":
		return "5-Minute Rate"
	case "15m.rate":
		return "15-Minute Rate"
	case "count":
		return "Count"
	case "max":
		return "Maximum"
	case "mean":
		return "Mean"
	case "mean.rate":
		return "Mean Rate"
	case "median":
		return "Median"
	case "min":
		return "Minimum"
	case "stddev":
		return "Standard Deviation"
	default:
		return sub
	}
}
