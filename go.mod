module knative.dev/eventing-kafka

go 1.15

require (
	github.com/Azure/azure-event-hubs-go/v3 v3.3.2
	github.com/Azure/azure-sdk-for-go v47.1.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.11.10 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.0 // indirect
	github.com/Shopify/sarama v1.28.0
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.2.0
	github.com/cloudevents/sdk-go/v2 v2.2.0
	github.com/davecgh/go-spew v1.1.1
	github.com/ghodss/yaml v1.0.0
	github.com/golang/protobuf v1.4.3
	github.com/google/go-cmp v0.5.5
	github.com/google/gofuzz v1.2.0
	github.com/google/uuid v1.2.0
	github.com/influxdata/tdigest v0.0.1 // indirect
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/mitchellh/mapstructure v1.3.3 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/slinkydeveloper/loadastic v0.0.0-20201218203601-5c69eea3b7d8
	github.com/stretchr/testify v1.7.0
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	go.opencensus.io v0.23.0
	go.uber.org/atomic v1.7.0
	go.uber.org/zap v1.16.0
	golang.org/x/sync v0.0.0-20201207232520-09787c993a3a
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e
	k8s.io/api v0.19.7
	k8s.io/apimachinery v0.19.7
	k8s.io/client-go v0.19.7
	k8s.io/utils v0.0.0-20200729134348-d5654de09c73
	knative.dev/eventing v0.21.1-0.20210324235118-724f4edb2a71
	knative.dev/hack v0.0.0-20210317214554-58edbdc42966
	knative.dev/networking v0.0.0-20210324061918-44a3b919bce1
	knative.dev/pkg v0.0.0-20210323202917-b558677ab034
)
