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
package statefulset

import (
	"math"
	"strconv"
	"strings"
)

func podNameFromOrdinal(name string, ordinal int32) string {
	return name + "-" + strconv.Itoa(int(ordinal))
}

func ordinalFromPodName(podName string) int32 {
	ordinal, err := strconv.ParseInt(podName[strings.LastIndex(podName, "-")+1:], 10, 32)
	if err != nil {
		return math.MaxInt32
	}
	return int32(ordinal)
}

func statefulSetName(podName string) string {
	return podName[:strings.LastIndex(podName, "-")-1]
}
