// +build !ignore_autogenerated

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

// Code generated by deepcopy-gen. DO NOT EDIT.

package v1beta1

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
	v1 "knative.dev/pkg/apis/duck/v1"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *KafkaSource) DeepCopyInto(out *KafkaSource) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new KafkaSource.
func (in *KafkaSource) DeepCopy() *KafkaSource {
	if in == nil {
		return nil
	}
	out := new(KafkaSource)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *KafkaSource) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *KafkaSourceList) DeepCopyInto(out *KafkaSourceList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]KafkaSource, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new KafkaSourceList.
func (in *KafkaSourceList) DeepCopy() *KafkaSourceList {
	if in == nil {
		return nil
	}
	out := new(KafkaSourceList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *KafkaSourceList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *KafkaSourceSpec) DeepCopyInto(out *KafkaSourceSpec) {
	*out = *in
	if in.Consumers != nil {
		in, out := &in.Consumers, &out.Consumers
		*out = new(int32)
		**out = **in
	}
	in.KafkaAuthSpec.DeepCopyInto(&out.KafkaAuthSpec)
	if in.Topics != nil {
		in, out := &in.Topics, &out.Topics
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	in.SourceSpec.DeepCopyInto(&out.SourceSpec)
	if in.Config != nil {
		in, out := &in.Config, &out.Config
		*out = new(v1.KReference)
		**out = **in
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new KafkaSourceSpec.
func (in *KafkaSourceSpec) DeepCopy() *KafkaSourceSpec {
	if in == nil {
		return nil
	}
	out := new(KafkaSourceSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *KafkaSourceStatus) DeepCopyInto(out *KafkaSourceStatus) {
	*out = *in
	in.SourceStatus.DeepCopyInto(&out.SourceStatus)
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new KafkaSourceStatus.
func (in *KafkaSourceStatus) DeepCopy() *KafkaSourceStatus {
	if in == nil {
		return nil
	}
	out := new(KafkaSourceStatus)
	in.DeepCopyInto(out)
	return out
}
