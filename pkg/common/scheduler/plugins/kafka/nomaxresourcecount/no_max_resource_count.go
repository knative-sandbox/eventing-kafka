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

package nomaxresourcecount

import (
	"context"

	"knative.dev/eventing-kafka/pkg/common/scheduler/factory"
	state "knative.dev/eventing-kafka/pkg/common/scheduler/state"
	"knative.dev/pkg/logging"
)

// NoMaxResourceCount plugin filters pods that cause total pods with placements
// to exceed total partitioncount
type NoMaxResourceCount struct {
}

// Verify NoMaxResourceCount Implements FilterPlugin Interface
var _ state.FilterPlugin = &NoMaxResourceCount{}

// Name of the plugin
const Name = state.NoMaxResourceCount

const (
	ErrReasonUnschedulable = "pod(s) were unschedulable"
)

func init() {
	factory.RegisterFP(Name, &NoMaxResourceCount{})
	//fmt.Println("NoMaxResourceCount plugin has been registered")
}

// Name returns name of the plugin
func (pl *NoMaxResourceCount) Name() string {
	return Name
}

// Filter invoked at the filter extension point.
func (pl *NoMaxResourceCount) Filter(ctx context.Context, states *state.State, podID int32) *state.Status {
	logger := logging.FromContext(ctx).With("Filter", pl.Name())

	logger.Infof("Pod %q passed %q predicate successfully", podID, pl.Name())
	return state.NewStatus(state.Success)
}
