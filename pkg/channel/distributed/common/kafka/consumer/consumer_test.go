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

package consumer

import (
	"strconv"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	kafkatesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/testing"
	commontesting "knative.dev/eventing-kafka/pkg/channel/distributed/common/testing"
	"knative.dev/eventing-kafka/pkg/common/client"
)

// Test Constants
const (
	ClientId      = "TestClientId"
	GroupId       = "TestGrouId"
	KafkaUsername = "TestUsername"
	KafkaPassword = "TestPassword"
)

// Test The CreateConsumerGroup() Functionality
func TestCreateConsumerGroup(t *testing.T) {

	// Test Data
	brokers := []string{"TestBroker"}

	// Replace The NewConsumerGroupWrapper With Mock For Testing & Restore After Test
	newConsumerGroupWrapperPlaceholder := NewConsumerGroupWrapper
	NewConsumerGroupWrapper = func(brokersArg []string, groupIdArg string, configArg *sarama.Config) (sarama.ConsumerGroup, error) {
		assert.Equal(t, brokers, brokersArg)
		assert.Equal(t, GroupId, groupIdArg)
		assert.NotNil(t, configArg)
		assert.Equal(t, ClientId, configArg.ClientID)
		assert.True(t, configArg.Net.SASL.Enable)
		assert.Equal(t, KafkaUsername, configArg.Net.SASL.User)
		assert.Equal(t, KafkaPassword, configArg.Net.SASL.Password)
		assert.Equal(t, constants.ConfigKafkaVersionDefault, configArg.Version)
		assert.Equal(t, commontesting.ConfigNetKeepAlive, strconv.FormatInt(int64(configArg.Net.KeepAlive), 10))
		assert.True(t, configArg.Consumer.Return.Errors)
		assert.Equal(t, commontesting.ConfigMetadataRefreshFrequency, strconv.FormatInt(int64(configArg.Metadata.RefreshFrequency), 10))
		assert.Equal(t, commontesting.ConfigConsumerOffsetsAutoCommitInterval, strconv.FormatInt(int64(configArg.Consumer.Offsets.AutoCommit.Interval), 10))
		assert.Equal(t, commontesting.ConfigConsumerOffsetsRetention, strconv.FormatInt(int64(configArg.Consumer.Offsets.Retention), 10))
		return kafkatesting.NewMockConsumerGroup(t), nil
	}
	defer func() {
		NewConsumerGroupWrapper = newConsumerGroupWrapperPlaceholder
	}()

	config, err := client.BuildSaramaConfig(nil, commontesting.SaramaDefaultConfigYaml, nil)
	assert.Nil(t, err)

	err = client.UpdateSaramaConfigWithKafkaAuthConfig(config, &client.KafkaAuthConfig{
		SASL: &client.KafkaSaslConfig{
			User:     KafkaUsername,
			Password: KafkaPassword,
		},
	})
	assert.Nil(t, err)

	config.ClientID = ClientId

	// Perform The Test
	consumerGroup, registry, err := CreateConsumerGroup(brokers, config, GroupId)

	// Verify The Results
	assert.Nil(t, err)
	assert.NotNil(t, consumerGroup)
	assert.NotNil(t, registry)
}
