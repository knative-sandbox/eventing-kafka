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

// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	"context"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
	v1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
)

// FakeKafkaChannels implements KafkaChannelInterface
type FakeKafkaChannels struct {
	Fake *FakeMessagingV1beta1
	ns   string
}

var kafkachannelsResource = schema.GroupVersionResource{Group: "messaging.knative.dev", Version: "v1beta1", Resource: "kafkachannels"}

var kafkachannelsKind = schema.GroupVersionKind{Group: "messaging.knative.dev", Version: "v1beta1", Kind: "KafkaChannel"}

// Get takes name of the kafkaChannel, and returns the corresponding kafkaChannel object, and an error if there is any.
func (c *FakeKafkaChannels) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1beta1.KafkaChannel, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(kafkachannelsResource, c.ns, name), &v1beta1.KafkaChannel{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1beta1.KafkaChannel), err
}

// List takes label and field selectors, and returns the list of KafkaChannels that match those selectors.
func (c *FakeKafkaChannels) List(ctx context.Context, opts v1.ListOptions) (result *v1beta1.KafkaChannelList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(kafkachannelsResource, kafkachannelsKind, c.ns, opts), &v1beta1.KafkaChannelList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1beta1.KafkaChannelList{ListMeta: obj.(*v1beta1.KafkaChannelList).ListMeta}
	for _, item := range obj.(*v1beta1.KafkaChannelList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested kafkaChannels.
func (c *FakeKafkaChannels) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(kafkachannelsResource, c.ns, opts))

}

// Create takes the representation of a kafkaChannel and creates it.  Returns the server's representation of the kafkaChannel, and an error, if there is any.
func (c *FakeKafkaChannels) Create(ctx context.Context, kafkaChannel *v1beta1.KafkaChannel, opts v1.CreateOptions) (result *v1beta1.KafkaChannel, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(kafkachannelsResource, c.ns, kafkaChannel), &v1beta1.KafkaChannel{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1beta1.KafkaChannel), err
}

// Update takes the representation of a kafkaChannel and updates it. Returns the server's representation of the kafkaChannel, and an error, if there is any.
func (c *FakeKafkaChannels) Update(ctx context.Context, kafkaChannel *v1beta1.KafkaChannel, opts v1.UpdateOptions) (result *v1beta1.KafkaChannel, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(kafkachannelsResource, c.ns, kafkaChannel), &v1beta1.KafkaChannel{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1beta1.KafkaChannel), err
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *FakeKafkaChannels) UpdateStatus(ctx context.Context, kafkaChannel *v1beta1.KafkaChannel, opts v1.UpdateOptions) (*v1beta1.KafkaChannel, error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceAction(kafkachannelsResource, "status", c.ns, kafkaChannel), &v1beta1.KafkaChannel{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1beta1.KafkaChannel), err
}

// Delete takes name of the kafkaChannel and deletes it. Returns an error if one occurs.
func (c *FakeKafkaChannels) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteAction(kafkachannelsResource, c.ns, name), &v1beta1.KafkaChannel{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeKafkaChannels) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(kafkachannelsResource, c.ns, listOpts)

	_, err := c.Fake.Invokes(action, &v1beta1.KafkaChannelList{})
	return err
}

// Patch applies the patch and returns the patched kafkaChannel.
func (c *FakeKafkaChannels) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1beta1.KafkaChannel, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(kafkachannelsResource, c.ns, name, pt, data, subresources...), &v1beta1.KafkaChannel{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1beta1.KafkaChannel), err
}
