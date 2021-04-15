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

package resources

import (
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/pkg/system"
)

const (
	DispatcherContainerName = "dispatcher"
	// TODO: move to common
	ConfigMapHashAnnotationKey = "kafka.eventing.knative.dev/configmap-hash"
)

var (
	dispatcherName   = "kafka-ch-dispatcher"
	dispatcherLabels = map[string]string{
		"messaging.knative.dev/channel": "kafka-channel",
		"messaging.knative.dev/role":    "dispatcher",
	}
)

type DispatcherArgs struct {
	DispatcherScope     string
	DispatcherNamespace string
	Image               string
	Replicas            int32
	ServiceAccount      string
	ConfigMapHash       string
}

// MakeDispatcher generates the dispatcher deployment for the KafKa channel
func MakeDispatcher(args DispatcherArgs) *v1.Deployment {
	replicas := args.Replicas

	return &v1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployments",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      dispatcherName,
			Namespace: args.DispatcherNamespace,
		},
		Spec: v1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: dispatcherLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: dispatcherLabels,
					Annotations: map[string]string{
						ConfigMapHashAnnotationKey: args.ConfigMapHash,
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: args.ServiceAccount,
					Containers: []corev1.Container{
						{
							Name:  DispatcherContainerName,
							Image: args.Image,
							Env:   makeEnv(args),
							Ports: []corev1.ContainerPort{{
								Name:          "metrics",
								ContainerPort: 9090,
							}},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      constants.SettingsConfigMapName,
									MountPath: constants.SettingsConfigMapMountPath,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: constants.SettingsConfigMapName,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: constants.SettingsConfigMapName,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func makeEnv(args DispatcherArgs) []corev1.EnvVar {
	vars := []corev1.EnvVar{{
		Name:  system.NamespaceEnvKey,
		Value: system.Namespace(),
	}, {
		Name:  "METRICS_DOMAIN",
		Value: "knative.dev/eventing",
	}, {
		Name:  "CONFIG_LOGGING_NAME",
		Value: "config-logging",
	}, {
		Name:  "CONFIG_LEADERELECTION_NAME",
		Value: "config-leader-election",
	}}

	if args.DispatcherScope == "namespace" {
		vars = append(vars, corev1.EnvVar{
			Name: "NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		}, corev1.EnvVar{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		}, corev1.EnvVar{
			Name:  "CONTAINER_NAME",
			Value: "dispatcher",
		})
	}

	return vars
}
