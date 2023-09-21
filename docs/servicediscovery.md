# Service Discovery
A core feature of PSC is automated service endpoint discovery, which allows PSC clients to connect to PubSub resources (e.g. topic) without the usual headaches that come with hardcoded hostnames/ports, incorrect protocol usage, and resource identification in multi-region / multi-cloud environments. Automated service endpoint discovery hides these complexities away from the client, and allows platform teams to standardize and manage the way in which clients connect to their servers.

If you haven't already, please take a look at [Resource Names (RNs) and Unique Resource Identifiers (URIs)](/docs/resourcenames.md) to understand those concepts before proceeding.

## How does it work?
Service discovery in PSC is managed by [ServiceDiscoveryManager.java](/psc/src/main/java/com/pinterest/psc/discovery/ServiceDiscoveryManager.java) which uses reflections to discover and register all classes annotated with `@ServiceDiscoveryPlugin`. These are plugin classes that implement the [ServiceDiscoveryProvider](/psc/src/main/java/com/pinterest/psc/discovery/ServiceDiscoveryProvider.java) interface. You are free to implement a `ServiceDiscoveryProvider` annotated with a specified plugin priority (e.g. `@ServiceDiscoveryPlugin(priority = 100)`), which returns a [ServiceDiscoveryConfig](/psc/src/main/java/com/pinterest/psc/common/ServiceDiscoveryConfig.java) that contains all the information that a PSC client needs to start producing/consuming data given a specified URI.

Feel free to read over those classes for a detailed look at how this is implemented.

## Example
In our [quickstart](/docs/quickstart.md) example, service discovery is done through the provider [ExampleKafkaServersetServiceDiscoveryProvider.java](/psc-examples/src/main/java/com/pinterest/psc/discovery/ExampleKafkaServersetServiceDiscoveryProvider.java) with high `@ServiceDiscoveryPlugin` priority to ensure that it is being used in runtime. This provider implementation simply takes the URI given to the client, dissects it into its component parts, and uses these parts to construct the filename of the expected [Serverset file](/psc-examples/quickstart-scripts/configs/local-region.discovery.local-cluster.serverset) which is already present in the filesystem.

In the serverset file, you will notice that there is only one endpoint: `localhost:9092` pointing to the local Kafka broker. The endpoint in this file is read, passed into the PSC client upon initialization, and used for producing/consuming data in our example clients.

In production environments with many possible endpoints, one could re-use this approach to construct dynamic serverset files that contain the endpoints of many hosts that are valid for client-server connection establishment, and place these serverset files on the hosts that clients are running on. As such, clients do not need to worry about service endpoint discovery - it is all automated!