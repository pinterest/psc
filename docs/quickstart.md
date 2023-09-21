# Quick Start Guide
Here is a set of instructions on how to launch your first PSC Java client, using Kafka 2.3.1 as an example backend PubSub system.

## Build
### Prerequisites
- JDK 8+
- Maven 3+
### Clone and build
```
> git clone https://github.com/pinterest/psc.git
> cd psc/java
> git checkout 2.3  # use PSC 2.3 for compatibility with Kafka 2.3.1
> mvn clean package -DskipTests
``` 

## Start local Kafka broker
To allow for PSC to publish or consume data from Kafka locally, start a local Kafka broker and create a test topic *my_test_topic*. The script [startlocalkafka.sh](../psc-examples/quickstart-scripts/startlocalkafka.sh) will achieve this for you.

```
> cd psc/java/psc-examples/quickstart-scripts

# download + run ZooKeeper and Kafka 2.3.1 and create a local Kafka topic my_test_topic
> ./startlocalkafka.sh  # non blocking once running; to stop use ./stoplocalkafka.sh
```

## Launching a PscProducer
There is a provided [ExamplePscProducer.java](../psc-examples/src/main/java/com/pinterest/psc/example/kafka/ExamplePscProducer.java) class that launches a simple PscProducer which writes some messages to *my_test_topic* our local Kafka broker. To launch it, simply execute

```
# assuming you're still in psc-examples/quickstart-scripts directory
> ./example_psc_producer.sh
```

## Launching a PscConsumer
There is a provided [ExamplePscConsumer.java](../psc-examples/src/main/java/com/pinterest/psc/example/kafka/ExamplePscConsumer.java) class that launches a simple PscConsumer which reads the messages that we just sent to *my_test_topic* our local Kafka broker. To launch it, simply execute

```
# assuming you're still in psc-examples/quickstart-scripts directory
> ./example_psc_consumer.sh
```

## Diving deeper

- [Resource Names (RNs)](/docs/resourcenames.md)
- [Service Discovery](/docs/servicediscovery.md)
- [Auto Remediation](/docs/autoremediation.md)
- [Native Kafka Client to PSC Migration](/docs/nativekafkatopscmigration.md)