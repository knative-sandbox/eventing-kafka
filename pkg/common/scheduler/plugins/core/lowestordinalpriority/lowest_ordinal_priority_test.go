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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	state "knative.dev/eventing-kafka/pkg/common/scheduler/state"
	tscheduler "knative.dev/eventing-kafka/pkg/common/scheduler/testing"
)

func TestFilter(t *testing.T) {
	testCases := []struct {
		name     string
		state    *state.State
		podID    int32
		expScore int64
		expected *state.Status
	}{
		{
			name:     "no vpods",
			state:    &state.State{LastOrdinal: -1},
			podID:    0,
			expScore: 100,
			expected: state.NewStatus(state.Success),
		},
		{
			name:     "one vpods free",
			state:    &state.State{LastOrdinal: 0},
			podID:    0,
			expScore: 200,
			expected: state.NewStatus(state.Success),
		},
		{
			name:     "two vpods free",
			state:    &state.State{LastOrdinal: 0},
			podID:    1,
			expScore: 199,
			expected: state.NewStatus(state.Success),
		},
		{
			name:     "one vpods not free",
			state:    &state.State{LastOrdinal: 1},
			podID:    0,
			expScore: 300,
			expected: state.NewStatus(state.Success),
		},
		{
			name:     "one vpods not free",
			state:    &state.State{LastOrdinal: 1},
			podID:    1,
			expScore: 299,
			expected: state.NewStatus(state.Success),
		},
		{
			name:     "many vpods, no gaps",
			state:    &state.State{LastOrdinal: 1},
			podID:    2,
			expScore: 298,
			expected: state.NewStatus(state.Success),
		},
		{
			name:     "many vpods, with gaps",
			state:    &state.State{LastOrdinal: 2},
			podID:    0,
			expScore: 400,
			expected: state.NewStatus(state.Success),
		},
		{
			name:     "many vpods, with gaps",
			state:    &state.State{LastOrdinal: 2},
			podID:    1000,
			expScore: 0,
			expected: state.NewStatus(state.Error, ErrReasonNegativeScore),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, _ := tscheduler.SetupFakeContext(t)
			var plugin = &LowestOrdinalPriority{}

			name := plugin.Name()
			assert.Equal(t, name, "LowestOrdinalPriority")

			score, status := plugin.Score(ctx, tc.state, tc.podID)
			if score != tc.expScore {
				t.Errorf("unexpected score, got %v, want %v", score, tc.expScore)
			}
			if !reflect.DeepEqual(status, tc.expected) {
				t.Errorf("unexpected status, got %v, want %v", status, tc.expected)
			}
		})
	}
}
