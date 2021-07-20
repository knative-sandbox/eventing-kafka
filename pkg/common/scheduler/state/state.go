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

package state

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	corev1 "k8s.io/client-go/listers/core/v1"
	"knative.dev/eventing-kafka/pkg/common/scheduler"
	"knative.dev/pkg/logging"
)

type StateAccessor interface {
	// State returns the current state (snapshot) about placed vpods
	// Take into account reserved vreplicas and update `reserved` to reflect
	// the current state.
	State(reserved map[types.NamespacedName]map[string]int32) (*State, error)
}

// state provides information about the current scheduling of all vpods
// It is used by for the scheduler and the autoscaler
type State struct {
	// free tracks the free capacity of each pod.
	FreeCap []int32

	// LastOrdinal is the ordinal index corresponding to the last statefulset replica
	// with placed vpods.
	LastOrdinal int32

	// Pod capacity.
	Capacity int32

	// Number of zones in cluster
	NumZones int32

	// Number of available nodes in cluster
	NumNodes int32

	// Scheduling policy type for placing vreplicas on pods
	SchedulerPolicy scheduler.SchedulerPolicyType

	// Mapping node names of nodes currently in cluster to their zone info
	NodeToZoneMap map[string]string
}

// Free safely returns the free capacity at the given ordinal
func (s *State) Free(ordinal int32) int32 {
	if int32(len(s.FreeCap)) <= ordinal {
		return s.Capacity
	}
	return s.FreeCap[ordinal]
}

// SetFree safely sets the free capacity at the given ordinal
func (s *State) SetFree(ordinal int32, value int32) {
	s.FreeCap = grow(s.FreeCap, ordinal, s.Capacity)
	s.FreeCap[int(ordinal)] = value
}

// freeCapacity returns the number of vreplicas that can be used,
// up to the last ordinal
func (s *State) FreeCapacity() int32 {
	t := int32(0)
	for i := int32(0); i <= s.LastOrdinal; i++ {
		t += s.FreeCap[i]
	}
	return t
}

// stateBuilder reconstruct the state from scratch, by listing vpods
type stateBuilder struct {
	ctx             context.Context
	logger          *zap.SugaredLogger
	vpodLister      scheduler.VPodLister
	capacity        int32
	schedulerPolicy scheduler.SchedulerPolicyType
	nodeLister      corev1.NodeLister
}

// NewStateBuilder returns a StateAccessor recreating the state from scratch each time it is requested
func NewStateBuilder(ctx context.Context, lister scheduler.VPodLister, podCapacity int32, schedulerPolicy scheduler.SchedulerPolicyType, nodeLister corev1.NodeLister) StateAccessor {
	return &stateBuilder{
		ctx:             ctx,
		logger:          logging.FromContext(ctx),
		vpodLister:      lister,
		capacity:        podCapacity,
		schedulerPolicy: schedulerPolicy,
		nodeLister:      nodeLister,
	}
}

func (s *stateBuilder) State(reserved map[types.NamespacedName]map[string]int32) (*State, error) {
	vpods, err := s.vpodLister()
	if err != nil {
		return nil, err
	}

	free := make([]int32, 0, 256)
	last := int32(-1)

	// keep track of (vpod key, podname) pairs with existing placements
	withPlacement := make(map[types.NamespacedName]map[string]bool)

	for _, vpod := range vpods {
		ps := vpod.GetPlacements()

		withPlacement[vpod.GetKey()] = make(map[string]bool)

		for i := 0; i < len(ps); i++ {
			podName := ps[i].PodName
			vreplicas := ps[i].VReplicas

			// Account for reserved vreplicas
			vreplicas = withReserved(vpod.GetKey(), podName, vreplicas, reserved)

			free, last = s.updateFreeCapacity(free, last, podName, vreplicas)

			withPlacement[vpod.GetKey()][podName] = true
		}
	}

	// Account for reserved vreplicas with no prior placements
	for key, ps := range reserved {
		for podName, rvreplicas := range ps {
			if wp, ok := withPlacement[key]; ok {
				if _, ok := wp[podName]; ok {
					// already accounted for
					break
				}
			}

			free, last = s.updateFreeCapacity(free, last, podName, rvreplicas)
		}
	}

	if s.schedulerPolicy == scheduler.EVENSPREAD || s.schedulerPolicy == scheduler.EVENSPREAD_BYNODE {
		//TODO: need a node watch to see if # nodes/ # zones have gone up or down
		nodes, err := s.nodeLister.List(labels.Everything())
		if err != nil {
			return nil, err
		}

		nodeToZoneMap := make(map[string]string)
		zoneMap := make(map[string]struct{})
		for i := 0; i < len(nodes); i++ {
			node := nodes[i]
			if node.Spec.Unschedulable {
				continue //ignore node that is currently unschedulable
			}
			zoneName, ok := node.GetLabels()[scheduler.ZoneLabel]
			if !ok {
				continue //ignore node that doesn't have zone info (maybe a test setup or control node)
			}

			nodeToZoneMap[node.Name] = zoneName
			zoneMap[zoneName] = struct{}{}
		}

		s.logger.Infow("cluster state info", zap.String("NumZones", fmt.Sprint(len(zoneMap))), zap.String("NumNodes", fmt.Sprint(len(nodeToZoneMap))))
		return &State{FreeCap: free, LastOrdinal: last, Capacity: s.capacity, NumZones: int32(len(zoneMap)), NumNodes: int32(len(nodeToZoneMap)), SchedulerPolicy: s.schedulerPolicy, NodeToZoneMap: nodeToZoneMap}, nil

	}
	return &State{FreeCap: free, LastOrdinal: last, Capacity: s.capacity, SchedulerPolicy: s.schedulerPolicy}, nil
}

func (s *stateBuilder) updateFreeCapacity(free []int32, last int32, podName string, vreplicas int32) ([]int32, int32) {
	ordinal := OrdinalFromPodName(podName)
	free = grow(free, ordinal, s.capacity)

	free[ordinal] -= vreplicas

	// Assert the pod is not overcommitted
	if free[ordinal] < 0 {
		// This should not happen anymore. Log as an error but do not interrupt the current scheduling.
		s.logger.Errorw("pod is overcommitted", zap.String("podName", podName), zap.Int32("free", free[ordinal]))
	}

	if ordinal > last && free[ordinal] != s.capacity {
		last = ordinal
	}

	return free, last
}

func grow(slice []int32, ordinal int32, def int32) []int32 {
	l := int32(len(slice))
	diff := ordinal - l + 1

	if diff <= 0 {
		return slice
	}

	for i := int32(0); i < diff; i++ {
		slice = append(slice, def)
	}
	return slice
}

func withReserved(key types.NamespacedName, podName string, committed int32, reserved map[types.NamespacedName]map[string]int32) int32 {
	if reserved != nil {
		if rps, ok := reserved[key]; ok {
			if rvreplicas, ok := rps[podName]; ok {
				if committed < rvreplicas {
					// new placement hasn't been committed yet. Adjust locally
					return rvreplicas
				} else if committed == rvreplicas {
					// new placement has been committed.
					delete(rps, podName)
					if len(rps) == 0 {
						delete(reserved, key)
					}
				}
				// else {
				// 	// the number of vreplicas has been reduced but not committed yet.
				// 	// do nothing.
				// }
			}
		}
	}
	return committed
}