/*
Copyright 2020 The Knative Authors

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

package v1beta1

import (
	"context"
	"testing"

	bindingsv1beta1 "knative.dev/eventing-kafka/contrib/kafka/source/pkg/apis/bindings/v1beta1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

var (
	fullSpec = KafkaSourceSpec{
		KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
			BootstrapServers: []string{"servers"},
		},
		Topics:        []string{"topics"},
		ConsumerGroup: "group",
		SourceSpec: duckv1.SourceSpec{
			Sink: duckv1.Destination{
				Ref: &duckv1.KReference{
					APIVersion: "foo",
					Kind:       "bar",
					Name:       "qux",
				},
			},
		},
	}
)

func TestKafkaSourceCheckImmutableFields(t *testing.T) {
	testCases := map[string]struct {
		orig    *KafkaSourceSpec
		updated KafkaSourceSpec
		allowed bool
	}{
		"nil orig": {
			updated: fullSpec,
			allowed: true,
		},
		"Topic changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				Topics:     []string{"some-other-topic"},
				SourceSpec: fullSpec.SourceSpec,
			},
			allowed: false,
		},
		"Bootstrap servers changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
					BootstrapServers: []string{"server1,server2"},
				},
				SourceSpec: fullSpec.SourceSpec,
			},
			allowed: false,
		},
		"Sink.APIVersion changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				Topics: fullSpec.Topics,
				SourceSpec: duckv1.SourceSpec{
					Sink: duckv1.Destination{
						Ref: &duckv1.KReference{
							APIVersion: "some-other-api-version",
							Kind:       fullSpec.Sink.Ref.APIVersion,
							Namespace:  fullSpec.Sink.Ref.Namespace,
							Name:       fullSpec.Sink.Ref.Name,
						},
					},
				},
			},
			allowed: false,
		},
		"Sink.Kind changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				Topics: fullSpec.Topics,
				SourceSpec: duckv1.SourceSpec{
					Sink: duckv1.Destination{
						Ref: &duckv1.KReference{
							APIVersion: fullSpec.Sink.Ref.APIVersion,
							Kind:       "some-other-kind",
							Namespace:  fullSpec.Sink.Ref.Namespace,
							Name:       fullSpec.Sink.Ref.Name,
						},
					},
				},
			},
			allowed: false,
		},
		"Sink.Namespace changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				Topics: fullSpec.Topics,
				SourceSpec: duckv1.SourceSpec{
					Sink: duckv1.Destination{
						Ref: &duckv1.KReference{
							APIVersion: fullSpec.Sink.Ref.APIVersion,
							Kind:       fullSpec.Sink.Ref.Kind,
							Namespace:  "some-other-namespace",
							Name:       fullSpec.Sink.Ref.Name,
						},
					},
				},
			},
			allowed: false,
		},
		"Sink.Name changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				Topics: fullSpec.Topics,
				SourceSpec: duckv1.SourceSpec{
					Sink: duckv1.Destination{
						Ref: &duckv1.KReference{
							APIVersion: fullSpec.Sink.Ref.APIVersion,
							Kind:       fullSpec.Sink.Ref.Kind,
							Namespace:  fullSpec.Sink.Ref.Namespace,
							Name:       "some-other-name",
						},
					},
				},
			},
			allowed: false,
		},
		"ServiceAccountName changed": {
			orig: &fullSpec,
			updated: KafkaSourceSpec{
				Topics: fullSpec.Topics,
				SourceSpec: duckv1.SourceSpec{
					Sink: duckv1.Destination{
						Ref: &duckv1.KReference{
							APIVersion: fullSpec.Sink.Ref.APIVersion,
							Kind:       fullSpec.Sink.Ref.Kind,
							Namespace:  fullSpec.Sink.Ref.Namespace,
							Name:       "some-other-name",
						},
					},
				},
			},
			allowed: false,
		},
		"no change": {
			orig:    &fullSpec,
			updated: fullSpec,
			allowed: true,
		},
	}

	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {
			ctx := context.TODO()
			if tc.orig != nil {
				orig := &KafkaSource{
					Spec: *tc.orig,
				}
				ctx = apis.WithinUpdate(ctx, orig)
			}
			updated := &KafkaSource{
				Spec: tc.updated,
			}

			err := updated.Validate(ctx)
			if tc.allowed != (err == nil) {
				t.Fatalf("Unexpected immutable field check. Expected %v. Actual %v", tc.allowed, err)
			}
		})
	}
}
