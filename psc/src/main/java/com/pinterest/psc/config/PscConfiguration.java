package com.pinterest.psc.config;

import com.pinterest.psc.consumer.PscConsumer;
import com.pinterest.psc.consumer.listener.MessageListener;
import com.pinterest.psc.discovery.ServiceDiscoveryProvider;
import com.pinterest.psc.logging.PscLogger;
import org.apache.commons.configuration2.PropertiesConfiguration;

/**
 * Various configurations for PSC client are defined here. There are a few configuration categories:
 * <ol>
 *     <li>Consumer: Configs that are prefixed with <code>psc.consumer.</code></li>
 *     <li>Producer: Configs that are prefixed with <code>psc.producer.</code></li>
 *     <li>Metrics: Configs that are prefixed with <code>psc.metrics.</code></li>
 *     <li>Environment: Configs that are prefixed with <code>code.environment.</code></li>
 *     <li>Discovery: Configs that are prefixed with <code>psc.discovery.</code></li>
 *     <li>Generic: Any other config that is prefixed with <code>psc.</code></li>
 * </ol>
 * <p>
 * The configurations that are listed here are not comprehensive. Specifically, for consumer and producer, only the
 * commonly used configurations and those with a different configuration name (than provided by the backend client
 * library) are added. For example, <code>KafkaConsumer</code> library supports a <code>enable.auto.commit</code> config.
 * PSC uses <code>commit.auto.enabled</code> instead. Therefore, this configuration is added here, and expected to be
 * used by {@link com.pinterest.psc.consumer.PscConsumer} clients.
 * <p>
 * Configurations that are not added in this class will be sent to the backend client library in a pass-through fashion.
 * For example, <code>KafkaConsumer</code> library supports a <code>security.protocol</code> config. When a
 * {@link com.pinterest.psc.consumer.PscConsumer} client uses
 * <pre>
 *     Configuration configuration = new Configuration();
 *     ...
 *     configuration.setString("psc.consumer.security.protocol", "PLAINTEXT");
 *     ...
 *     PscConsumer pscConsumer = new PscConsumer(configuration);
 * </pre>
 * since <code>security.protocol</code> is not among the consumer configs defined in this class, it will be passed on
 * and used when launching the backend client instance.
 * <p>
 * If a configuration is allowed to have an empty value and is mapped to a backend client configuration, the default
 * for the backend client configuration will be used. For example, to override the default set by
 * <code>PscConsumer</code> library with the <code>KafkaConsumer</code> default, simply override the config with an
 * empty string value.
 * <p>
 * A comma is considered a separator in configuration values, and turn the specific configuration value into an array
 * list. The comma separator is required for some configurations that can be multi-valued, e.g.
 * {@value PSC_CONSUMER_INTERCEPTORS_RAW_CLASSES}. Therefore, to avoid misconfiguration of any single-value config do
 * not use a comma in the config value.
 */
public class PscConfiguration extends PropertiesConfiguration {
    private static final PscLogger logger = PscLogger.getLogger(PscConfiguration.class);

    public final static String PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST = "earliest";
    public final static String PSC_CONSUMER_OFFSET_AUTO_RESET_LATEST = "latest";
    public final static String PSC_CONSUMER_OFFSET_AUTO_RESET_NONE = "none";

    public final static String PSC_CONSUMER_ISOLATION_LEVEL_NON_TRANSACTIONAL = "read_uncommitted";
    public final static String PSC_CONSUMER_ISOLATION_LEVEL_TRANSACTIONAL = "read_committed";

    /**
     * This is meant to be the id of the project that is using the PSC instance.
     */
    public final static String PSC_PROJECT = "psc.project";

    /**
     * If this config is set to true, it will enable logging of PSC configuration to the PSC config topic
     * set via <code>psc.config.topic.uri</code>. Default is false. If an explicit value is not provided, for non-cloud
     * hosts (e.g. local machines) it defaults to <code>false</code> (disabled config logging), and for cloud hosts
     * (e.g. ec2) it defaults to <code>true</code> (enabled config logging). This means except for when an explicit
     * value of <code>false</code> is provided, the config <code>psc.config.topic.uri</code> must be present with a
     * valid topic URI to guarantee a smooth transition from development machine to deployment hosts.
     */
    public final static String PSC_CONFIG_LOGGING_ENABLED = "psc.config.logging.enabled";

    /**
     * This topic URI, if provided, is used to send JSON-formatted PSC configurations to.
     */
    public final static String PSC_CONFIG_TOPIC_URI = "psc.config.topic.uri";

    /**
     * Whether to enable auto resolution attempts (via retries or resets of clients) for certain failures. This is
     * enabled by default, and uses an incremental backoff period in between tries. The formula for the length of
     * each backoff <code>2^retry * 200 ms</code>, which means backoff periods are 400 ms, 800 ms, 1600 ms, ..., in
     * order.
     */
    public final static String PSC_AUTO_RESOLUTION_ENABLED = "psc.auto.resolution.enabled";

    /**
     * The number of attempts to make for auto resolution of failures. Default is 5.
     */
    public final static String PCS_AUTO_RESOLUTION_RETRY_COUNT = "psc.auto.resolution.retry.count";

    /**
     * Whether to proactively reset consumer or producer based on approaching SSL certificate expiry
     */
    public final static String PSC_PROACTIVE_SSL_RESET_ENABLED = "psc.proactive.ssl.reset.enabled";

    private final static String PSC_CLIENT_TYPE = "psc.client.type";
    public final static String PSC_CLIENT_TYPE_CONSUMER = "consumer";
    public final static String PSC_CLIENT_TYPE_PRODUCER = "producer";
    private final static String[] PSC_VALID_CLIENT_TYPES = {PSC_CLIENT_TYPE_CONSUMER, PSC_CLIENT_TYPE_PRODUCER};


    // **********************
    // Consumer Configuration
    // **********************

    protected static final String PSC_CONSUMER = "psc.consumer";
    protected static final String ASSIGNMENT_STRATEGY_CLASS = "assignment.strategy.class";
    protected static final String BUFFER_RECEIVE_BYTES = "buffer.receive.bytes";
    protected static final String BUFFER_SEND_BYTES = "buffer.send.bytes";
    protected static final String CLIENT_ID = "client.id";
    protected static final String COMMIT_AUTO_ENABLED = "commit.auto.enabled";
    protected static final String GROUP_ID = "group.id";
    protected static final String INTERCEPTORS_RAW_CLASSES = "interceptors.raw.classes";
    protected static final String INTERCEPTORS_TYPED_CLASSES = "interceptors.typed.classes";
    protected static final String ISOLATION_LEVEL = "isolation.level";
    protected static final String KEY_DESERIALIZER = "key.deserializer";
    protected static final String MESSAGE_LISTENER = "message.listener";
    protected static final String METADATA_AGE_MAX_MS = "metadata.age.max.ms";
    protected static final String OFFSET_AUTO_RESET = "offset.auto.reset";
    protected static final String PARTITION_FETCH_MAX_BYTES = "partition.fetch.max.bytes";
    protected static final String POLL_INTERVAL_MAX_MS = "poll.interval.max.ms";
    protected static final String POLL_MESSAGES_MAX = "poll.messages.max";
    protected static final String POLL_TIMEOUT_MS = "poll.timeout.ms";
    protected static final String VALUE_DESERIALIZER = "value.deserializer";

    // security-related configs
    protected static final String SECURITY_PROTOCOL = "security.protocol";
    protected static final String SSL_ENABLED_PROTOCOLS = "ssl.enabled.protocols";
    protected static final String SSL_KEY_PASSWORD = "ssl.key.password";
    protected static final String SSL_KEYSTORE_LOCATION = "ssl.keystore.location";
    protected static final String SSL_KEYSTORE_PASSWORD = "ssl.keystore.password";
    protected static final String SSL_KEYSTORE_TYPE = "ssl.keystore.type";
    protected static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
    protected static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
    protected static final String SSL_TRUSTSTORE_TYPE = "ssl.truststore.type";

    /**
     * The consumer configuration {@value PSC_CONSUMER_ASSIGNMENT_STRATEGY_CLASS} expects the FQDN of the class that
     * performs assignment of partitions to consumers in the consumer group. Defaults to empty string so the backend
     * client default value is used.
     */
    public static final String PSC_CONSUMER_ASSIGNMENT_STRATEGY_CLASS = PSC_CONSUMER + "." + ASSIGNMENT_STRATEGY_CLASS;

    /**
     * The consumer configuration {@value PSC_CONSUMER_BUFFER_RECEIVE_BYTES} expects a numeric value as the size of the
     * TCP receive buffer to use when reading data. If the value is -1, the OS default will be used. Defaults to 1 MB.
     */
    public static final String PSC_CONSUMER_BUFFER_RECEIVE_BYTES = PSC_CONSUMER + "." + BUFFER_RECEIVE_BYTES;

    /**
     * The consumer configuration {@value PSC_CONSUMER_BUFFER_SEND_BYTES} expects a numeric value as the size of the
     * TCP send buffer to use when sending data. If the value is -1, the OS default will be used. Defaults to 1 MB.
     */
    public static final String PSC_CONSUMER_BUFFER_SEND_BYTES = PSC_CONSUMER + "." + BUFFER_SEND_BYTES;

    /**
     * The consumer configuration {@value PSC_CONSUMER_CLIENT_ID} expects a string that identifies the
     * {@link com.pinterest.psc.consumer.PscConsumer} instance. This is a mandatory configuration with no default value.
     * Note that, to avoid name clash when this client id is passed on to build potentially several backend consumer
     * client instances, PSC adds a random suffix to it to make it unique for each backend instance.
     */
    public static final String PSC_CONSUMER_CLIENT_ID = PSC_CONSUMER + "." + CLIENT_ID;

    /**
     * The consumer configuration {@value PSC_CONSUMER_COMMIT_AUTO_ENABLED} expects a true or false value that
     * determines whether consumer offsets should be automatically committed or not. Defaults to empty string so the
     * backend client default value is used.
     */
    public static final String PSC_CONSUMER_COMMIT_AUTO_ENABLED = PSC_CONSUMER + "." + COMMIT_AUTO_ENABLED;

    /**
     * The consumer configuration {@value PSC_CONSUMER_GROUP_ID} expects the name of the consumer group this
     * {@link com.pinterest.psc.consumer.PscConsumer} instance will belong to. This config used to scale the consumption
     * among multiple consumer instances. This is a mandatory configuration with no default value.
     */
    public static final String PSC_CONSUMER_GROUP_ID = PSC_CONSUMER + "." + GROUP_ID;

    /**
     * The consumer configuration {@value PSC_CONSUMER_INTERCEPTORS_RAW_CLASSES} expects a comma separated list of FQDN
     * for custom interceptor classes that work with raw bytes of a message (before deserialization). Depending on which
     * API they override, these raw interceptors will be applied in order and before any other interceptor is applied.
     */
    public static final String PSC_CONSUMER_INTERCEPTORS_RAW_CLASSES = PSC_CONSUMER + "." + INTERCEPTORS_RAW_CLASSES;

    /**
     * The consumer configuration {@value PSC_CONSUMER_INTERCEPTORS_TYPED_CLASSES} expects a comma separated list of
     * FQDN for custom interceptor classes that work with typed message (after deserialization). Depending on which API
     * they override, these typed interceptors will be applied in order and after all other interceptors are applied.
     */
    public static final String PSC_CONSUMER_INTERCEPTORS_TYPED_CLASSES = PSC_CONSUMER + "." + INTERCEPTORS_TYPED_CLASSES;

    /**
     * The consumer configuration {@value PSC_CONSUMER_ISOLATION_LEVEL} expects one of <code>read_uncommitted</code>
     * or <code>read_committed</code>, that determines whether the consumer should read without or with transactional
     * consideration. While <code>read_uncommitted</code> returns all messages, <code>read_committed</code> returns only
     * messages that are committed by a transactional producer (i.e. a producer that sends messages transactionally).
     * Defaults to empty string so that the backend library default is used.
     */
    public static final String PSC_CONSUMER_ISOLATION_LEVEL = PSC_CONSUMER + "." + ISOLATION_LEVEL;

    /**
     * The consumer configuration {@value PSC_CONSUMER_KEY_DESERIALIZER} expects either the FQDN of the
     * deserializer class for message keys or a deserializer class object that is instantiated in the client
     * application. Defaults to <code>com.pinterest.psc.serde.ByteArrayDeserializer</code>.
     */
    public static final String PSC_CONSUMER_KEY_DESERIALIZER = PSC_CONSUMER + "." + KEY_DESERIALIZER;

    /**
     * The consumer configuration {@value PSC_CONSUMER_MESSAGE_LISTENER} expects either the FQDN of the message listener
     * class that implements the {@link MessageListener} interface or an instance of that class that is instantiated in
     * the client application. Defaults to empty string, which means no listener class is used.
     */
    public static final String PSC_CONSUMER_MESSAGE_LISTENER = PSC_CONSUMER + "." + MESSAGE_LISTENER;

    /**
     * The consumer configuration {@value PSC_CONSUMER_METADATA_AGE_MAX_MS} expects a numeric value as the period of
     * time in milliseconds before forcing a metadata refresh to proactively discover new brokers or partitions.
     * Defaults to empty string so that the backend library default is used.
     */
    public static final String PSC_CONSUMER_METADATA_AGE_MAX_MS = PSC_CONSUMER + "." + METADATA_AGE_MAX_MS;

    /**
     * The consumer configuration {@value PSC_CONSUMER_OFFSET_AUTO_RESET} expects one of
     * <ul>
     *     <li><code>PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_EARLIEST (or "earliest")</code></li>
     *     <li><code>PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_LATEST (or "latest")</code></li>
     *     <li><code>PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_NONE (or "none")</code></li>
     * </ul>
     * to determine where the consumer should start consuming the log if
     * there is no prior consumer offset to resume from. Defaults to Defaults to empty string so the backend client
     * default value is used.
     */
    public static final String PSC_CONSUMER_OFFSET_AUTO_RESET = PSC_CONSUMER + "." + OFFSET_AUTO_RESET;

    /**
     * The consumer configuration {@value PSC_CONSUMER_PARTITION_FETCH_MAX_BYTES} expects a numeric value as the
     * maximum amount of data per-partition the server will return. Records are fetched in batches by the consumer.
     * If the first record batch in the first non-empty partition of the fetch is larger than this limit, the batch
     * will still be returned to ensure that the consumer can make progress. Defaults to empty string so the backend
     * client default value is used.
     */
    public static final String PSC_CONSUMER_PARTITION_FETCH_MAX_BYTES = PSC_CONSUMER + "." + PARTITION_FETCH_MAX_BYTES;

    /**
     * The consumer configuration {@value PSC_CONSUMER_POLL_INTERVAL_MAX_MS} expects a value in milliseconds as the
     * maximum delay between calls to <code>poll()</code> when the consumer belongs to a consumer group. Defaults to
     * empty string so that the backend library default is used.
     */
    public static final String PSC_CONSUMER_POLL_INTERVAL_MAX_MS = PSC_CONSUMER + "." + POLL_INTERVAL_MAX_MS;

    /**
     * The consumer configuration {@value PSC_CONSUMER_POLL_MESSAGES_MAX} expects a number that specifies the maximum
     * number of messages to return after each "backend" <code>poll</code> invocation. Defaults to <code>500</code>.
     * Note that if the <code>PscConsumer</code> instance handles <code>N</code> backend consumer, the maximum number
     * of messages returned from a {@link PscConsumer#poll()} will be <code>N</code> times the value of this config.
     */
    public static final String PSC_CONSUMER_POLL_MESSAGES_MAX = PSC_CONSUMER + "." + POLL_MESSAGES_MAX;

    /**
     * The consumer configuration {@value PSC_CONSUMER_POLL_TIMEOUT_MS} expects a milliseconds value and is used to cap
     * the maximum time waiting for a "backend" <code>poll()</code> call to return. Defaults to <code>500</code>. Note
     * that the maximum wait time for {@link PscConsumer#poll()} will be slightly higher as it has to aggregate and
     * prepare messages returned from individual backend consumers.
     */
    public static final String PSC_CONSUMER_POLL_TIMEOUT_MS = PSC_CONSUMER + "." + POLL_TIMEOUT_MS;

    /**
     * The consumer configuration {@value PSC_CONSUMER_VALUE_DESERIALIZER} expects either the FQDN of the deserializer
     * class for message values or a deserializer class object that is instantiated in the client application. Defaults
     * to <code>com.pinterest.psc.serde.ByteArrayDeserializer</code>.
     */
    public static final String PSC_CONSUMER_VALUE_DESERIALIZER = PSC_CONSUMER + "." + VALUE_DESERIALIZER;

    /*
    public static final String PSC_CONSUMER_SECURITY_PROTOCOL = PSC_CONSUMER + "." + SECURITY_PROTOCOL;

    public static final String PSC_CONSUMER_SSL_ENABLED_PROTOCOLS = PSC_CONSUMER + "." + SSL_ENABLED_PROTOCOLS;

    public static final String PSC_CONSUMER_SSL_KEY_PASSWORD = PSC_CONSUMER + "." + SSL_KEY_PASSWORD;

    public static final String PSC_CONSUMER_SSL_KEYSTORE_LOCATION = PSC_CONSUMER + "." + SSL_KEYSTORE_LOCATION;

    public static final String PSC_CONSUMER_SSL_KEYSTORE_PASSWORD = PSC_CONSUMER + "." + SSL_KEYSTORE_PASSWORD;

    public static final String PSC_CONSUMER_SSL_KEYSTORE_TYPE = PSC_CONSUMER + "." + SSL_KEYSTORE_TYPE;

    public static final String PSC_CONSUMER_SSL_TRUSTSTORE_LOCATION = PSC_CONSUMER + "." + SSL_TRUSTSTORE_LOCATION;

    public static final String PSC_CONSUMER_SSL_TRUSTSTORE_PASSWORD = PSC_CONSUMER + "." + SSL_TRUSTSTORE_PASSWORD;

    public static final String PSC_CONSUMER_SSL_TRUSTSTORE_TYPE = PSC_CONSUMER + "." + SSL_TRUSTSTORE_TYPE;
    */


    // **********************
    // Producer Configuration
    // **********************

    protected static final String PSC_PRODUCER = "psc.producer";
    protected static final String KEY_SERIALIZER = "key.serializer.class";
    protected static final String VALUE_SERIALIZER = "value.serializer.class";
    protected static final String ACKS = "acks";
    protected static final String BATCH_DURATION_MAX_MS = "batch.duration.max.ms";
    protected static final String BATCH_SIZE_BYTES = "batch.size.bytes";
    protected static final String BUFFER_MEMORY_BYTES = "buffer.memory.bytes";
    protected static final String IDEMPOTENCE_ENABLED = "idempotence.enabled";
    protected static final String INFLIGHT_REQUESTS_PER_CONNECTION_MAX = "inflight.requests.per.connection.max";
    protected static final String REQUEST_SIZE_MAX_BYTES = "request.size.max.bytes";
    protected static final String TRANSACTIONAL_ID = "transactional.id";
    protected static final String TRANSACTION_TIMEOUT_MS = "transaction.timeout.ms";
    protected static final String RETRIES = "retries";

    /**
     * The producer configuration {@value PSC_PRODUCER_ACKS} expects the number of acknowledgements required by the
     * producer before considering a request complete and successful. It controls the durability of records that are
     * sent.
     * <ul>
     *     <li>A value of <code>0</code> means no ack is required; i.e. the producer will not wait for any
     *     acknowledgment from the backend pubsub server at all (fire and forget).</li>
     *     <li>A value of 1 means an acknowledgement from server side will be send as soon as one copy of the sent
     *     message is persisted (favors availability).</li>
     *     <li>A value of <code>-1</code> or <code>all</code> means all replicas on the backend pubsub must acknowledge
     *     persisting the message before the producer assumes the send is successful (favors consistency).</li>
     * </ul>
     * Defaults to 1.
     */
    public static final String PSC_PRODUCER_ACKS = PSC_PRODUCER + "." + ACKS;

    /**
     * The producer configuration {@value PSC_PRODUCER_BATCH_DURATION_MAX_MS} expects a numerical value that specifies
     * the maximum amount of artificial delay, so rather than the producer immediately sending out a record it will
     * wait for up to the given delay to allow other records to be sent so that the sends can be batched together. This
     * configuration translates to <code>linger.ms</code> configuration of Kafka producer. Defaults to 100 ms.
     */
    public static final String PSC_PRODUCER_BATCH_DURATION_MAX_MS = PSC_PRODUCER + "." + BATCH_DURATION_MAX_MS;

    /**
     * The producer configuration {@value PSC_PRODUCER_BATCH_SIZE_BYTES} expects a numerical value that controls the
     * default batch size in bytes. Producer batches requests together to reduce number of requests sent to the backend.
     * Defaults to empty string so the backend client default is used.
     */
    public static final String PSC_PRODUCER_BATCH_SIZE_BYTES = PSC_PRODUCER + "." + BATCH_SIZE_BYTES;

    /**
     * The producer configuration {@value PSC_PRODUCER_BUFFER_MEMORY_BYTES} expects a numerical value that corresponds
     * roughly to the total memory the producer will use (excludes memory used for compression and in-flight requests).
     * Defaults to backend client default value.
     */
    public static final String PSC_PRODUCER_BUFFER_MEMORY_BYTES = PSC_PRODUCER + "." + BUFFER_MEMORY_BYTES;

    /**
     * The producer configuration {@value PSC_PRODUCER_BUFFER_RECEIVE_BYTES} expects a numerical value for the size of
     * the TCP receive buffer to use when reading data. If the value is -1, the OS default will be used. Defaults to
     * backend client default value.
     */
    public static final String PSC_PRODUCER_BUFFER_RECEIVE_BYTES = PSC_PRODUCER + "." + BUFFER_RECEIVE_BYTES;

    /**
     * The producer configuration {@value PSC_PRODUCER_BUFFER_SEND_BYTES} expects a numerical value for the size of the
     * TCP send buffer to use when sending data. If the value is -1, the OS default will be used. Defaults to backend
     * client default value.
     */
    public static final String PSC_PRODUCER_BUFFER_SEND_BYTES = PSC_PRODUCER + "." + BUFFER_SEND_BYTES;

    /**
     * The producer configuration {@value PSC_PRODUCER_CLIENT_ID} expects a string that identifies the
     * {@link com.pinterest.psc.producer.PscProducer} instance. Defaults to <code>default-psc-producer-client-id</code>,
     * but is expected to be custom defined per application. Note that, to avoid name clash when this client id is
     * passed on to build potentially several backend consumer client instances, PSC uses a random suffix to make it
     * unique for each backend instance.
     */
    public static final String PSC_PRODUCER_CLIENT_ID = PSC_PRODUCER + "." + CLIENT_ID;

    /**
     * The producer configuration {@value PSC_PRODUCER_IDEMPOTENCE_ENABLED} expects a true/false value. When set to
     * 'true', the producer will ensure that exactly one copy of each message is written in the stream. If 'false',
     * producer retries due to broker failures, etc., may write duplicates of the retried message in the stream.
     * Defaults to empty string so the backend client default is used.
     */
    public static final String PSC_PRODUCER_IDEMPOTENCE_ENABLED = PSC_PRODUCER + "." + IDEMPOTENCE_ENABLED;

    /**
     * The producer configuration {@value PSC_PRODUCER_INFLIGHT_REQUESTS_PER_CONNECTION_MAX} expects a numeric value
     * that specifies the maximum number of unacknowledged requests the client will send on a single connection before
     * blocking. Note that if this setting is set to be greater than 1 and there are failed sends, there is a risk of
     * message re-ordering due to retries (i.e., if retries are enabled). Defaults to backend client default value.
     */
    public static final String PSC_PRODUCER_INFLIGHT_REQUESTS_PER_CONNECTION_MAX =
            PSC_PRODUCER + "." + INFLIGHT_REQUESTS_PER_CONNECTION_MAX;

    /**
     * The producer configuration {@value PSC_PRODUCER_INTERCEPTORS_RAW_CLASSES} expects a comma separated list of FQDN
     * for custom interceptor classes that work with raw bytes of a message (after serialization). Depending on which
     * API they override, these raw interceptors will be applied in order and after all other interceptors are applied.
     */
    public static final String PSC_PRODUCER_INTERCEPTORS_RAW_CLASSES = PSC_PRODUCER + "." + INTERCEPTORS_RAW_CLASSES;

    /**
     * The producer configuration {@value PSC_PRODUCER_INTERCEPTORS_TYPED_CLASSES} expects a comma separated list of
     * FQDN for custom interceptor classes that work with typed message (before serialization). Depending on which API
     * they override, these typed interceptors will be applied in order and before any other interceptor is applied.
     */
    public static final String PSC_PRODUCER_INTERCEPTORS_TYPED_CLASSES = PSC_PRODUCER + "." + INTERCEPTORS_TYPED_CLASSES;

    /**
     * The producer configuration {@value PSC_PRODUCER_KEY_SERIALIZER} expects either the FQDN of the
     * serializer class for message keys or a serializer class object that is instantiated in the client
     * application. Defaults to <code>com.pinterest.psc.serde.ByteArraySerializer</code>.
     */
    public static final String PSC_PRODUCER_KEY_SERIALIZER = PSC_PRODUCER + "." + KEY_SERIALIZER;

    /**
     * The producer configuration {@value PSC_PRODUCER_METADATA_AGE_MAX_MS} expects a numerical value that specifies the
     * period of time in milliseconds before forcing a refresh of metadata to proactively discover any new brokers or
     * partitions. Defaults to backend client default value.
     */
    public static final String PSC_PRODUCER_METADATA_AGE_MAX_MS = PSC_PRODUCER + "." + METADATA_AGE_MAX_MS;

    /**
     * The producer configuration {@value PSC_PRODUCER_REQUEST_SIZE_MAX_BYTES} expects a numerical value for the
     * maximum size of a request in bytes. This setting will limit the number of record batches the producer will send
     * in a single request to avoid sending huge requests. Defaults to empty string so the backend client default value
     * is used.
     */
    public static final String PSC_PRODUCER_REQUEST_SIZE_MAX_BYTES = PSC_PRODUCER + "." + REQUEST_SIZE_MAX_BYTES;

    /**
     * The producer configuration {@value PSC_PRODUCER_RETRIES} expects a numerical value for the maximum number of
     * retries the backend producer internally attempts in case <code>send()</code> returns an error from the backend
     * PubSub. Defaults to empty string so the backend client default value is used.
     * If auto resolution configuration is set to <code>true</code>, the number of retries should be set to a low
     * number to make sure the backend producer eventually errors out after it exhausts those retries. For example, the
     * default retries for <code>KafkaProducer</code> is max int which practically disables auto-resolution as the error
     * would not surface to <code>PscProducer</code>.
     */
    public static final String PSC_PRODUCER_RETRIES = PSC_PRODUCER + "." + RETRIES;

    /**
     * The producer configuration {@value PSC_PRODUCER_TRANSACTIONAL_ID} expects a string value for the
     * TransactionalId to use for transactional delivery based on the backend pubsub delivery semantics.
     * Defaults to empty string so the backend client default value is used.
     */
    public static final String PSC_PRODUCER_TRANSACTIONAL_ID = PSC_PRODUCER + "." + TRANSACTIONAL_ID;

    /**
     * The producer configuration {@value PSC_PRODUCER_TRANSACTION_TIMEOUT_MS} expects a numerical value for the
     * maximum amount of time in ms that a transaction can be active without sending a heartbeat or a status update.
     * Defaults to empty string so the backend client default value is used.
     */
    public static final String PSC_PRODUCER_TRANSACTION_TIMEOUT_MS = PSC_PRODUCER + "." + TRANSACTION_TIMEOUT_MS;

    /**
     * The producer configuration {@value PSC_PRODUCER_VALUE_SERIALIZER} expects either the FQDN of the
     * serializer class for message values or a serializer class object that is instantiated in the client
     * application. Defaults to <code>com.pinterest.psc.serde.ByteArraySerializer</code>.
     */
    public static final String PSC_PRODUCER_VALUE_SERIALIZER = PSC_PRODUCER + "." + VALUE_SERIALIZER;

    /*
     * The producer configuration {@value PSC_PRODUCER_SECURITY_PROTOCOL} expects the security protocol for producer
     * writes. Defaults to <code>PLAINTEXT</code>.
     */
    /*
    public static final String PSC_PRODUCER_SECURITY_PROTOCOL = PSC_PRODUCER + "." + SECURITY_PROTOCOL;

    public static final String PSC_PRODUCER_SSL_ENABLED_PROTOCOLS = PSC_PRODUCER + "." + SSL_ENABLED_PROTOCOLS;

    public static final String PSC_PRODUCER_SSL_KEY_PASSWORD = PSC_PRODUCER + "." + SSL_KEY_PASSWORD;

    public static final String PSC_PRODUCER_SSL_KEYSTORE_LOCATION = PSC_PRODUCER + "." + SSL_KEYSTORE_LOCATION;

    public static final String PSC_PRODUCER_SSL_KEYSTORE_PASSWORD = PSC_PRODUCER + "." + SSL_KEYSTORE_PASSWORD;

    public static final String PSC_PRODUCER_SSL_KEYSTORE_TYPE = PSC_PRODUCER + "." + SSL_KEYSTORE_TYPE;

    public static final String PSC_PRODUCER_SSL_TRUSTSTORE_LOCATION = PSC_PRODUCER + "." + SSL_TRUSTSTORE_LOCATION;

    public static final String PSC_PRODUCER_SSL_TRUSTSTORE_PASSWORD = PSC_PRODUCER + "." + SSL_TRUSTSTORE_PASSWORD;

    public static final String PSC_PRODUCER_SSL_TRUSTSTORE_TYPE = PSC_PRODUCER + "." + SSL_TRUSTSTORE_TYPE;
    */


    // **********************
    // Metrics Configuration
    // **********************

    protected static final String PSC_METRICS = "psc.metrics";
    protected static final String REPORTING_ENABLED = "reporting.enabled";
    protected static final String REPORTER_CLASS = "reporter.class";
    protected static final String REPORTER_PARALLELISM = "reporter.parallelism";
    protected static final String HOST = "host";
    protected static final String PORT = "port";
    protected static final String FREQUENCY_MS = "frequency.ms";

    /**
     * {@value PSC_METRIC_REPORTING_ENABLED} expects a true/false value to indicate whether PSC should emit metrics that
     * it collects, or not. The default is 'true'.
     */
    public static final String PSC_METRIC_REPORTING_ENABLED = PSC_METRICS + "." + REPORTING_ENABLED;

    /**
     * {@value PSC_METRICS_REPORTER_CLASS} expects the FQDN of the metrics reporter class. If an explicit value is not
     * provided, for non-cloud hosts (e.g. local machines) it defaults to
     * <code>com.pinterest.psc.metrics.NullMetricsReporter</code> (disabled metrics reporting), and for cloud hosts
     * (e.g. ec2) it defaults to <code>com.pinterest.psc.metrics.OpenTSDBMetricsReporter</code> (enabled metrics
     * reporting).
     */
    public static final String PSC_METRICS_REPORTER_CLASS = PSC_METRICS + "." + REPORTER_CLASS;

    /**
     * {@value PSC_METRICS_REPORTER_PARALLELISM} expects a number that defines how many reporter threads should be
     * created for each PSC client. If several clients are running on a single host a smaller number should be used to
     * reduce the chance of conflicts among the running threads. The default is 10.
     */
    public static final String PSC_METRICS_REPORTER_PARALLELISM = PSC_METRICS + "." + REPORTER_PARALLELISM;

    /**
     * {@value PSC_METRICS_HOST} expects the resolvable host name or IP address of the host to send the metrics to.
     * Defaults to <code>127.0.0.1</code>, the local host.
     */
    public static final String PSC_METRICS_HOST = PSC_METRICS + "." + HOST;

    /**
     * {@value PSC_METRICS_PORT} expects the port number to which the metrics agent is listening. Defaults to
     * <code>18126</code>.
     */
    public static final String PSC_METRICS_PORT = PSC_METRICS + "." + PORT;

    /**
     * {@value PSC_METRICS_FREQUENCY_MS} expects the frequency of emitting metrics in milliseconds. Defaults to
     * <code>60000</code>, or 60 seconds.
     */
    public static final String PSC_METRICS_FREQUENCY_MS = PSC_METRICS + "." + FREQUENCY_MS;


    // **********************
    // Environment Configuration
    // **********************

    protected static final String PSC_ENVIRONMENT = "psc.environment";
    protected static final String PROVIDER_CLASS = "provider.class";

    /**
     * {@value PSC_ENVIRONMENT_PROVIDER_CLASS} expects the FQDN of the class that implements the
     * {@link com.pinterest.psc.environment.EnvironmentProvider} interface for extracting information about the host
     * this consumer is running on. Defaults to <code>com.pinterest.psc.environment.LocalEnvironmentProvider</code>.
     */
    public static final String PSC_ENVIRONMENT_PROVIDER_CLASS = PSC_ENVIRONMENT + "." + PROVIDER_CLASS;


    // **********************
    // Service Discovery Configuration
    // **********************

    protected static final String PSC_DISCOVERY = "psc.discovery";
    protected static final String FALLBACK_FILE = "fallback.file";

    /**
     * {@value PSC_DISCOVERY_FALLBACK_FILE} expects the file path that is used as the fallback option if none of the
     * provided priority based implementations of {@link ServiceDiscoveryProvider} can discover the backend pubsub
     * cluster to provide information such as bootstrap or seed brokers, security protocol, etc. An example of this file
     * is provided in the file <code>discovery.json.example</code>.
     */
    public static final String PSC_DISCOVERY_FALLBACK_FILE = PSC_DISCOVERY + "." + FALLBACK_FILE;

    public PscConfiguration() {
        super();
    }

}
