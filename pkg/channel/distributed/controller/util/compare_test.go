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
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/constants"
	logtesting "knative.dev/pkg/logging/testing"
)

type deploymentOption func(service *appsv1.Deployment)

// Tests the CheckDeploymentChanged functionality.  Note that this is also tested fairly extensively
// as part of the various reconciler tests, and as such the deployment structs used here are somewhat trivial.
func TestCheckDeploymentChanged(t *testing.T) {
	logger := logtesting.TestLogger(t).Desugar()
	tests := []struct {
		name               string
		existingDeployment *appsv1.Deployment
		newDeployment      *appsv1.Deployment
		expectUpdated      bool
	}{
		{
			name:               "Different Image",
			existingDeployment: getBasicDeployment(),
			newDeployment:      getBasicDeployment(withDifferentImage),
			expectUpdated:      true,
		},
		{
			name:               "Missing Required Label",
			existingDeployment: getBasicDeployment(),
			newDeployment:      getBasicDeployment(withLabel),
			expectUpdated:      true,
		},
		{
			name:               "Missing Existing Container",
			existingDeployment: getBasicDeployment(withoutContainer),
			newDeployment:      getBasicDeployment(),
			expectUpdated:      true,
		},
		{
			name:               "Extra Existing Label",
			existingDeployment: getBasicDeployment(withLabel),
			newDeployment:      getBasicDeployment(),
		},
		{
			name:               "Missing New Container",
			existingDeployment: getBasicDeployment(),
			newDeployment:      getBasicDeployment(withoutContainer),
		},
		{
			name:               "Multiple Existing Containers",
			existingDeployment: getBasicDeployment(withExtraContainer),
			newDeployment:      getBasicDeployment(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newDeployment := tt.newDeployment
			if newDeployment == nil {
				newDeployment = getBasicDeployment()
			}
			updatedDeployment, isUpdated := CheckDeploymentChanged(logger, tt.existingDeployment, tt.newDeployment)
			assert.NotNil(t, updatedDeployment)
			assert.Equal(t, tt.expectUpdated, isUpdated)
		})
	}
}

func getBasicDeployment(options ...deploymentOption) *appsv1.Deployment {
	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       constants.DeploymentKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "TestDeployment",
			Namespace: "TestNamespace",
			Labels:    make(map[string]string),
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "TestContainerName"},
					},
				},
			},
		},
	}

	// Apply any desired customizations
	for _, option := range options {
		option(deployment)
	}

	return deployment
}

func withLabel(deployment *appsv1.Deployment) {
	deployment.Labels["TestLabelName"] = "TestLabelValue"
}

func withoutContainer(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers = []corev1.Container{}
}

func withExtraContainer(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers = append(
		deployment.Spec.Template.Spec.Containers, corev1.Container{
			Name: "TestExtraContainerName",
		})
}

func withDifferentImage(deployment *appsv1.Deployment) {
	deployment.Spec.Template.Spec.Containers[0].Image = "TestNewImage"
}
