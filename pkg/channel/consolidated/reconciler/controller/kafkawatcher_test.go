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

package controller

import (
	"sort"
	"sync"
	"testing"
	"time"

	pkgtesting "knative.dev/pkg/logging/testing"
)

//TODO how to mock the sarama AdminClient
type FakeClusterAdmin struct {
	mutex sync.RWMutex
	cgs   []string
}

func (fake *FakeClusterAdmin) ListConsumerGroups() ([]string, error) {
	fake.mutex.RLock()
	defer fake.mutex.RUnlock()
	cgs := fake.cgs
	return cgs, nil
}

func (fake *FakeClusterAdmin) deleteCG(cg string) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	cgs := make([]string, len(fake.cgs)-1)
	for _, c := range fake.cgs {
		if c != cg {
			cgs = append(cgs, c)
		}
	}
	fake.cgs = cgs
}

func TestKafkaWatcher(t *testing.T) {
	cgname := "kafka.event-example.default-kne-trigger.0d9c4383-1e68-42b5-8c3a-3788274404c5"
	wid := "channel-abc"
	cgs := []string{cgname}
	ca := FakeClusterAdmin{
		cgs: cgs,
	}

	ch := make(chan []string, 1)

	w := NewKafkaWatcher(pkgtesting.TestContextWithLogger(t), &ca, 2*time.Second)
	w.Watch(wid, func() {
		cgs := w.List(func(cg string) bool {
			return cgname == cg
		})
		ch <- cgs
	})

	w.Start()
	<-ch
	assertSync(t, ch, cgs)
	ca.deleteCG(cgname)
	assertSync(t, ch, []string{})
}

func assertSync(t *testing.T, ch chan []string, cgs []string) {
	select {
	case syncedCGs := <-ch:
		if !equal(syncedCGs, cgs) {
			t.Errorf("observed and expected consumer groups do not match. got %v expected %v", syncedCGs, cgs)
		}
	case <-time.After(6 * time.Second):
		t.Errorf("timedout waiting for consumer groups to sync")
	}
}

func equal(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Strings(a)
	sort.Strings(b)

	for i, s := range a {
		if s != b[i] {
			return false
		}
	}
	return true
}
