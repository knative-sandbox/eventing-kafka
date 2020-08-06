module knative.dev/eventing-kafka

go 1.14

require (
	github.com/Azure/azure-event-hubs-go v1.3.1
	github.com/Shopify/sarama v1.26.4
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.2.0
	github.com/cloudevents/sdk-go/v2 v2.2.0
	github.com/google/go-cmp v0.5.1
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0
	github.com/slok/goresilience v0.2.0
	github.com/stretchr/testify v1.5.1
	go.opencensus.io v0.22.5-0.20200716030834-3456e1d174b2
	go.uber.org/zap v1.15.0
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	k8s.io/api v0.18.7-rc.0
	k8s.io/apimachinery v0.18.7-rc.0
	k8s.io/client-go v12.0.0+incompatible
	knative.dev/eventing v0.16.1-0.20200805125228-0b129d3eca14
	knative.dev/eventing-contrib v0.16.1-0.20200805184428-a87b96ae9579
	knative.dev/pkg v0.0.0-20200805045628-bb91a15b1a18
	knative.dev/test-infra v0.0.0-20200803175002-5efff0c4bd0a
	pack.ag/amqp v0.12.4 // indirect
)

replace (
	github.com/Azure/go-autorest => github.com/Azure/go-autorest v12.3.0+incompatible
	github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 => github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2 v2.0.1-0.20200608152019-2ab697c8fc0b
	github.com/cloudevents/sdk-go/v2 => github.com/cloudevents/sdk-go/v2 v2.0.1-0.20200608152019-2ab697c8fc0b
	k8s.io/api => k8s.io/api v0.17.6
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.17.6
	k8s.io/apimachinery => k8s.io/apimachinery v0.17.6
	k8s.io/client-go => k8s.io/client-go v0.17.6
	k8s.io/code-generator => k8s.io/code-generator v0.17.6
)
