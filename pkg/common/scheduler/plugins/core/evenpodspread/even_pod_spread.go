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

package evenpodspread

import (
	"context"

	"knative.dev/eventing-kafka/pkg/common/scheduler/factory"
	state "knative.dev/eventing-kafka/pkg/common/scheduler/state"
	"knative.dev/pkg/logging"
)

// EvenPodSpread is a filter plugin that eliminates pods that do not create an equal spread of resources across pods
type EvenPodSpread struct {
}

// Verify EvenPodSpread Implements FilterPlugin Interface
var _ state.FilterPlugin = &EvenPodSpread{}

// Name of the plugin
const Name = state.EvenPodSpread

func init() {
	factory.RegisterFP(Name, &EvenPodSpread{})
	//fmt.Println("EvenPodSpread plugin has been registered")
}

// Name returns name of the plugin
func (pl *EvenPodSpread) Name() string {
	return Name
}

// Filter invoked at the filter extension point.
func (pl *EvenPodSpread) Filter(ctx context.Context, states *state.State, podID int32) *state.Status {
	logger := logging.FromContext(ctx).With("Filter", pl.Name())

	logger.Infof("Pod %q passed %q predicate successfully", podID, pl.Name())
	return state.NewStatus(state.Success)
}
