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

package lowestordinalpriority

import (
	"context"

	"knative.dev/eventing-kafka/pkg/common/scheduler/factory"
	state "knative.dev/eventing-kafka/pkg/common/scheduler/state"
	"knative.dev/pkg/logging"
)

// LowestOrdinalPriority is a score plugin that favors pods that create an even spread of resources across zones for HA
type LowestOrdinalPriority struct {
}

// Verify LowestOrdinalPriority Implements ScorePlugin Interface
var _ state.ScorePlugin = &LowestOrdinalPriority{}

// Name of the plugin
const Name = state.LowestOrdinalPriority

const (
	ErrReasonNegativeScore = "computed score is negative"
)

func init() {
	factory.RegisterSP(Name, &LowestOrdinalPriority{})
	//fmt.Println("LowestOrdinalPriority plugin has been registered")
}

// Name returns name of the plugin
func (pl *LowestOrdinalPriority) Name() string {
	return Name
}

// Score invoked at the score extension point.
func (pl *LowestOrdinalPriority) Score(ctx context.Context, states *state.State, podID int32) (int64, *state.Status) {
	logger := logging.FromContext(ctx).With("Score", pl.Name())

	score := int64(((states.LastOrdinal + 2) * 100) - podID) //lower ordinals get higher score
	if score < 0 {
		return 0, state.NewStatus(state.Error, ErrReasonNegativeScore)
	}

	logger.Infof("Pod %q scored by %q priority successfully", podID, pl.Name())
	return score, state.NewStatus(state.Success)
}

// ScoreExtensions of the Score plugin.
func (pl *LowestOrdinalPriority) ScoreExtensions() state.ScoreExtensions {
	return pl
}

// NormalizeScore invoked after scoring all pods.
func (pl *LowestOrdinalPriority) NormalizeScore(ctx context.Context, states *state.State, scores state.PodScoreList) *state.Status {
	//no normalizing to perform for this plugin
	return state.NewStatus(state.Success)
}
