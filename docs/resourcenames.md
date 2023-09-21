# Resource Names (RNs) & Unique Resource Identifiers (URIs)
Resource names (RNs) are introduced in PSC as first-class citizens. Each RN uniquely identifies a resource (e.g. Kafka topic). By extending the RN, Unique Resource Identifiers (URIs) allow PSC to figure out how to establish a connection to it in order to produce data or consume data.

Some similar concepts you may already know of:

- URLs (Unique Resource Locators) can uniquely identify resources on the Internet and allow your browser to connect to them
- AWS ARNs (Amazon Resource Names) can uniquely identify AWS resources

## Motivation
In the modern data landscape, resources such as PubSub topics can be scattered among a vast collection of cloud hosts across multiple data centers and regions. In many cases, the topology is non-static in order to meet scaling needs. 

The traditional way of identifying these resources, for example connecting to a Kafka topic through the native Kafka client via `bootstrap.servers`, is prone to issues when considering the dynamic and global nature of today's data resources. We are introducing RNs and URIs as a way to: 

1. Increase dev velocity by freeing application teams from the burden of having to understand service discovery mechanisms
2. Reduce KTLO load on platform teams by standardizing resource identification and service discovery across all clients
3. Future-proof large scale data resource management by providing a flexible and extensible convention for resource identification and endpoint discovery

## RN vs URI

Resource Names (RNs) uniquely identify a resource through a schema that contains important information such as

- The type of service it belongs to (e.g. Kafka or MemQ)
- The environment it is running in (e.g. dev or prod)
- The associated cloud provider and region (e.g. aws_us-west-1)
- The classifier that can be used for custom scenarios (e.g. FGAC datasets)
- The name of the cluster it is running on (e.g. a pubsub cluster name)
- The name of the resource itself (e.g. the topic name)

This yields a schema for an RN that looks something like:
```
rn:<service>:<environment>:<cloud_region>:<classifier>:<cluster>:<topic>
```

In reality, an RN often not enough to establish a client-server connection. A URI adds a protocol and separator that specifies the way that a client should establish a connection to the RN. For example, some RNs may be accessible via plaintext connection (`plaintext:/`), while others may require TLS connection (`secure:/`). This is similar to the protocol specifier in a URL (e.g. `http://` vs. `https://`).

As a result, a URI will look something like:
```
<protocol>:/<rn>
```

As an example, a full topic URI that can be passed to a PSC client might look something like:
```
secure:/rn:kafka:prod:aws_us-west-1:pii:cluster01:my_topic
```

## Usage
Assuming that the [ServiceDiscoveryProviders](/psc/src/main/java/com/pinterest/psc/discovery/) are appropriately implemented for your environment, a well-formed URI is all that a PSC client needs in order to establish a connection to a PubSub topic. This means there is no longer a need for each client to understand service discovery, track the hostnames that it needs to connect to, and specify configs related to TLS connections. These can all be abstracted away and hidden from the client.

For example, in the [ExamplePscConsumer.java](/psc-examples/src/main/java/com/pinterest/psc/example/kafka/ExamplePscConsumer.java) client, notice that PSC no longer requires `bootstrap.servers` to be specified in the consumer configuration, unlike the native Kafka client. Platform teams can now manage the `ServiceDiscoveryProviders` that allow for endpoint discovery that is fully automated and less error-prone. In the example, service discovery logic is encapsulated in the ExampleServersetServiceDiscoveryProvider. The client just needs the topic's URI and PSC handles the rest underneath the hood.
```
PscConfiguration pscConfiguration = new PscConfiguration();
pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-psc-consumer-group");
pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID, "test-psc-consumer-client");
pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_KEY_DESERIALIZER, IntegerDeserializer.class.getName());
pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_VALUE_DESERIALIZER, StringDeserializer.class.getName());
pscConfiguration.setProperty(PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET, PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST);

PscConsumer<Integer, String> pscConsumer = new PscConsumer<>(pscConfiguration);
pscConsumer.subscribe(Collections.singleton(topicUri));
```

Similarly, a PscProducer needs nothing more than the topic's URI in `send()`:

```
PscConfiguration pscConfiguration = new PscConfiguration();
pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "test-psc-producer-client");
pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_KEY_SERIALIZER, IntegerSerializer.class.getName());
pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_VALUE_SERIALIZER, StringSerializer.class.getName());

PscProducer<Integer, String> pscProducer = new PscProducer<>(pscConfiguration);
PscProducerMessage<Integer, String> message = new PscProducerMessage<>(topicUri, "hello world");
pscProducer.send(message);
```

A detailed explanation of automated service endpoint discovery can be found here: [Service Discovery](/docs/servicediscovery.md)


## Additional Links
Please take a look at [TopicRn.java](/psc/src/main/java/com/pinterest/psc/common/TopicRn.java) and [TopicUri.java](/psc/src/main/java/com/pinterest/psc/common/TopicUri.java) for more information about their implementation and expected usage.

RNs and URIs enable [automated service endpoint discovery](/docs/servicediscovery.md), a core feature in PSC.

## Non-PubSub Use Cases

Adoption of RNs and URIs do not need to be limited to just PubSub resources, although PSC offers them as first-class citizens. 

Just as PubSub topics can be uniquely identified via its RN, and connections to them can be established via the additional information contained in the URI, other resources such as tables and databases can also leverage the benefits of RNs and URIs.