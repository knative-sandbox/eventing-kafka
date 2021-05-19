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

package kafkasecretinformer

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	informerscorev1 "k8s.io/client-go/informers/core/v1"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	commonconstants "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/eventing-kafka/pkg/channel/distributed/controller/env"
	"knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"
)

//
// Custom Kafka SecretInformer - Namespace & Label Restricted
//
// Note:  The default generated Knative Informers/Listers (e.g. SecretInformer) are all started against the same
//        context/factory which is scoped as cluster-wide.  This means the Listers require RBAC permissions to
//        access their resources in any namespace.  This is desirable for watching Services, Deployments, etc.,
//        but is not desirable for watching Secrets for obvious reasons.  We're only interested in "Kafka"
//        Secrets in the "System" namespace where eventing-kafka is installed (i.e. knative-eventing).
//        Therefore we've created the following custom "KafkaSecretInformer" which is based on a separate
//        factory which is namespaced and tweaked to restrict focus to only "Kafka" Secrets.
//

// Add The InformerInjector Function With The Knative Injection Framework
func init() {
	injection.Default.RegisterInformer(withInformer)
}

// Key Used To Associate The Informer Inside The Context
type Key struct{}

// Custom InformerInjector For KafkaSecretInformer
func withInformer(ctx context.Context) (context.Context, controller.Informer) {

	// Determine The "System" Namespace Where Eventing-Kafka Is Running
	namespace := system.Namespace()

	// Define The SharedInformerOptions To Restrict To Specified Namespace & With KafkaSecret Label
	sharedInformerOptions := []informers.SharedInformerOption{
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(listOptions *metav1.ListOptions) {
			listOptions.LabelSelector = fmt.Sprintf("%s=true", commonconstants.KafkaSecretLabel)
		}),
	}

	// Create A SharedInformerFactory With The Namespaced / Labelled Options
	logger := logging.FromContext(ctx).Desugar()
	environment, err := env.GetEnvironment(logger)
	if err != nil {
		logger.Fatal("Failed To Load Environment Variables - Terminating!", zap.Error(err))
	}
	namespacedSharedInformerFactory := informers.NewSharedInformerFactoryWithOptions(
		client.Get(ctx), environment.ResyncPeriod, sharedInformerOptions...)

	// Create The Custom Kafka SecretInformer
	kafkaSecretInformer := KafkaSecretInformer{
		namespace: namespace,
		factory:   namespacedSharedInformerFactory,
	}

	// Add To The Specified Context & Return The Informer
	return context.WithValue(ctx, Key{}, kafkaSecretInformer), kafkaSecretInformer.Informer()
}

// Extract The Typed Kafka SecretInformer From The Specified Context
func Get(ctx context.Context) informerscorev1.SecretInformer {
	untyped := ctx.Value(Key{})
	if untyped == nil {
		logging.FromContext(ctx).Panic("Unable to fetch eventing-kafka/pkg/controller/kafkasecret/KafkaSecretInformer from context.")
	}
	return untyped.(informerscorev1.SecretInformer)
}

// Verify The KafkaSecretInformer Implements The K8S SecretInformer Interface
var _ informerscorev1.SecretInformer = KafkaSecretInformer{}

// Custom Kafka SecretInformer Implementation
type KafkaSecretInformer struct {
	namespace string
	factory   informers.SharedInformerFactory
}

// Implement The K8S SecretInformer's Informer() Interface Function
func (i KafkaSecretInformer) Informer() cache.SharedIndexInformer {
	return i.factory.Core().V1().Secrets().Informer()
}

// Implement The K8S SecretInformer's Lister() Interface Function
func (i KafkaSecretInformer) Lister() listerscorev1.SecretLister {
	return listerscorev1.NewSecretLister(i.Informer().GetIndexer())
}