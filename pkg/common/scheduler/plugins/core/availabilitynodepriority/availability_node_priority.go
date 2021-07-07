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

package availabilitynodepriority

import (
	"context"
	"fmt"

	"knative.dev/eventing-kafka/pkg/common/scheduler/factory"
	state "knative.dev/eventing-kafka/pkg/common/scheduler/state"
	"knative.dev/pkg/logging"
)

// AvailabilityNodePriority is a score plugin that favors pods that create an even spread of resources across nodes for HA
type AvailabilityNodePriority struct {
}

// Verify AvailabilityNodePriority Implements ScorePlugin Interface
var _ state.ScorePlugin = &AvailabilityNodePriority{}

// Name of the plugin
const Name = state.AvailabilityNodePriority

func init() {
	factory.RegisterSP(Name, &AvailabilityNodePriority{}) //TODO: Not working
	fmt.Println("AvailabilityNodePriority has been registered")
}

// Name returns name of the plugin
func (pl *AvailabilityNodePriority) Name() string {
	return Name
}

// Score invoked at the score extension point.
func (pl *AvailabilityNodePriority) Score(ctx context.Context, states *state.State, podID int32) (int64, *state.Status) {
	logger := logging.FromContext(ctx).With("Score", pl.Name())

	score := calculatePriority()

	logger.Infof("Pod %q scored by %q priority successfully", podID, pl.Name())
	return score, state.NewStatus(state.Success)
}

// ScoreExtensions of the Score plugin.
func (pl *AvailabilityNodePriority) ScoreExtensions() state.ScoreExtensions {
	return pl
}

// NormalizeScore invoked after scoring all pods.
func (pl *AvailabilityNodePriority) NormalizeScore(ctx context.Context, state *state.State, scores state.PodScoreList) *state.Status {
	return nil
}

// calculatePriority returns the priority of a pod. Given the ...
func calculatePriority() int64 {
	return 1
}
