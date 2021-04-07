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

package util

import (
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
)

// Modifies An Existing Deployment With New Fields (If Necessary)
// Returns True If Any Modifications Were Made
func CheckDeploymentChanged(logger *zap.Logger, existingDeployment, newDeployment *appsv1.Deployment) (*appsv1.Deployment, bool) {

	// Make a copy of the existing labels so we don't inadvertently modify the existing deployment fields directly
	updatedLabels := make(map[string]string)
	for oldKey, oldValue := range existingDeployment.ObjectMeta.Labels {
		updatedLabels[oldKey] = oldValue
	}

	// Add any labels in the "new" deployment to the copy of the labels from the old deployment.
	// Annotations could be similarly updated, but there are currently no annotations being made
	// in new deployments anyway so it would serve no practical purpose at the moment.
	labelsChanged := false
	for newKey, newValue := range newDeployment.ObjectMeta.Labels {
		oldValue, ok := existingDeployment.ObjectMeta.Labels[newKey]
		if !ok || oldValue != newValue {
			labelsChanged = true
			updatedLabels[newKey] = newValue
		}
	}

	// Fields intentionally ignored:
	//    Spec.Replicas - Since a HorizontalPodAutoscaler explicitly changes this value on the deployment directly

	// Verify everything in the container spec aside from some particular exceptions (see "ignoreFields" below)
	existingContainerCount := len(existingDeployment.Spec.Template.Spec.Containers)
	if existingContainerCount == 0 {
		// This is unlikely but if it happens, replace the entire existing deployment with a proper one
		logger.Error("Deployment Has No Containers")
		return newDeployment, true
	} else if existingContainerCount > 1 {
		logger.Warn("Deployment Has Multiple Containers; Comparing First Only")
	}
	if len(newDeployment.Spec.Template.Spec.Containers) < 1 {
		logger.Error("New Deployment Has No Containers")
		return existingDeployment, false
	}
	existingContainer := &existingDeployment.Spec.Template.Spec.Containers[0]
	newContainer := &newDeployment.Spec.Template.Spec.Containers[0]

	// Ignore the fields in a Container struct which are not set directly by the distributed channel reconcilers
	// and ones that are acceptable to be changed manually (such as the ImagePullPolicy)
	ignoreFields := cmpopts.IgnoreFields(*newContainer,
		"Lifecycle",
		"TerminationMessagePolicy",
		"ImagePullPolicy",
		"SecurityContext",
		"StartupProbe",
		"TerminationMessagePath",
		"Stdin",
		"StdinOnce",
		"TTY")

	containersEqual := cmp.Equal(existingContainer, newContainer, ignoreFields)
	if containersEqual && !labelsChanged {
		// Nothing of interest changed, so just keep the existing deployment
		return existingDeployment, false
	}

	// Create an updated deployment from the existing one, but using the new Container field
	updatedDeployment := existingDeployment.DeepCopy()
	if labelsChanged {
		updatedDeployment.ObjectMeta.Labels = updatedLabels
	}
	if !containersEqual {
		updatedDeployment.Spec.Template.Spec.Containers[0] = *newContainer
	}
	return updatedDeployment, true
}
