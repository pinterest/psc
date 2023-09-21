package com.pinterest.psc.metrics;


public class PscMetrics {
    public static final String PSC_METRICS_REGISTRY_BASE_NAME = "psc";
    public static final String PSC_BACKEND_METRICS_PREFIX = "backend.";

    public static final String PSC_METRICS_REPORTER_COUNT = "metrics.reporter.count";

    public static final String PSC_CONSUMER_AUTO_RESOLUTION_RETRY_SUCCESS = "consumer.auto.resolution.retry.success";
    public static final String PSC_CONSUMER_AUTO_RESOLUTION_RETRY_FAILURE = "consumer.auto.resolution.retry.failure";
    public static final String PSC_PRODUCER_AUTO_RESOLUTION_RETRY_SUCCESS = "producer.auto.resolution.retry.success";
    public static final String PSC_PRODUCER_AUTO_RESOLUTION_RETRY_FAILURE = "producer.auto.resolution.retry.failure";

    // consumer
    public static final String PSC_CONSUMER_COUNT = "consumer.count";
    public static final String PSC_CONSUMER_POLL_TIME_MS_METRIC = "consumer.poll.time.ms";
    public static final String PSC_CONSUMER_POLL_MESSAGES_METRIC = "consumer.poll.messages";
    public static final String PSC_CONSUMER_POLL_KEYED_MESSAGES_METRIC = "consumer.poll.keyed.messages";
    public static final String PSC_CONSUMER_POLL_NULL_MESSAGE_VALUES_METRIC = "consumer.poll.null.value.messages";
    public static final String PSC_CONSUMER_POLL_INBOUND_BYTES_METRIC = "consumer.poll.inbound.bytes";
    public static final String PSC_CONSUMER_POLL_MESSAGE_KEY_SIZE_BYTES_METRIC = "consumer.poll.message.key.size.bytes";
    public static final String PSC_CONSUMER_POLL_MESSAGE_VALUE_SIZE_BYTES_METRIC = "consumer.poll.message.value.size.bytes";
    public static final String PSC_CONSUMER_TIME_LAG_MS_METRIC = "consumer.time.lag.ms";
    public static final String PSC_CONSUMER_OFFSET_MESSAGES_METRIC = "consumer.offset.messages";
    public static final String PSC_CONSUMER_RESETS_METRIC = "consumer.resets.count";
    public static final String PSC_CONSUMER_RETRIES_METRIC = "consumer.retries.count";
    public static final String PSC_CONSUMER_RETRIES_REACHED_LIMIT_METRIC = "consumer.retries.reached.limit.count";

    // producer
    public static final String PSC_PRODUCER_COUNT = "producer.count";
    public static final String PSC_PRODUCER_PRODUCE_TIME_MS_METRIC = "producer.produce.time.ms";
    public static final String PSC_PRODUCER_PRODUCE_MESSAGES_METRIC = "producer.produce.messages";
    public static final String PSC_PRODUCER_ACKED_MESSAGES_METRIC = "producer.acked.messages";
    public static final String PSC_PRODUCER_PRODUCE_KEYED_MESSAGES_METRIC = "producer.produce.keyed.messages";
    public static final String PSC_PRODUCER_PRODUCE_NULL_VALUE_MESSAGES_METRIC = "producer.produce.null.value.messages";
    public static final String PSC_PRODUCER_PRODUCE_MESSAGE_KEY_SIZE_BYTES_METRIC = "producer.produce.message.key.size.bytes";
    public static final String PSC_PRODUCER_PRODUCE_MESSAGE_VALUE_SIZE_BYTES_METRIC = "producer.produce.message.value.size.bytes";
    public static final String PSC_PRODUCER_RESETS_METRIC = "producer.resets.count";
    public static final String PSC_PRODUCER_RETRIES_METRIC = "producer.retries.count";
    public static final String PSC_PRODUCER_RETRIES_REACHED_LIMIT_METRIC = "producer.retries.reached.limit.count";

    // backend_producer
    public static final String PSC_PRODUCER_BACKEND_COUNT = "producer.backend.count";
    public static final String PSC_PRODUCER_BACKEND_SEND_ATTEMPT_COUNT = "producer.send.attempt.count";
    public static final String PSC_PRODUCER_BACKEND_DECOMMISSIONED_COUNT = "producer.decommissioned.count";

    // warnings
    public static final String PSC_BACKEND_PRODUCER_CONFIG_OVERWRITE = "producer.config.overwrite.count";
    public static final String PSC_BACKEND_CONSUMER_CONFIG_OVERWRITE = "consumer.config.overwrite.count";

    // secure protocol
    public static final String PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_SUCCESS = "secure.ssl.keystore.read.success.count";
    public static final String PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_SUCCESS = "secure.ssl.truststore.read.success.count";
    public static final String PSC_BACKEND_SECURE_SSL_KEYSTORE_READ_FAILURE = "secure.ssl.keystore.read.failure.count";
    public static final String PSC_BACKEND_SECURE_SSL_TRUSTSTORE_READ_FAILURE = "secure.ssl.truststore.read.failure.count";
    public static final String PSC_BACKEND_SECURE_SSL_CERTIFICATE_EXPIRY_TIME_METRIC = "secure.ssl.certificate.expiry.time";
}
