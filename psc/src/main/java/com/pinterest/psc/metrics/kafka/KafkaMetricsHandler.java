package com.pinterest.psc.metrics.kafka;

import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.kafka.KafkaTopicUri;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import com.pinterest.psc.logging.PscLogger;

import java.util.HashMap;
import java.util.Map;

public class KafkaMetricsHandler {
    private static final PscLogger logger = PscLogger.getLogger(KafkaMetricsHandler.class);
    private static final Map<String, Map<String, String>> kafkaConsumerMetrics = new HashMap<>();
    private static final Map<String, Map<String, String>> kafkaProducerMetrics = new HashMap<>();

    static {
        // consumer
        Map<String, String> internalMap = new HashMap<>(23, 1);
        internalMap.put("assigned-partitions", "consumer.coordinator.assigned.partitions");
        internalMap.put("commit-latency-avg", "consumer.coordinator.commit.latency.ms.avg");
        internalMap.put("commit-latency-max", "consumer.coordinator.commit.latency.ms.max");
        internalMap.put("commit-rate", "consumer.coordinator.commit.rate.per.second");
        internalMap.put("commit-total", "consumer.coordinator.commit.total.per.second");
        internalMap.put("failed-rebalance-total", "consumer.coordinator.failed.rebalance.total");
        internalMap.put("heartbeat-rate", "consumer.coordinator.heartbeat.rate.per.second");
        internalMap.put("heartbeat-response-time-max", "consumer.coordinator.heartbeat.response.time.ms.max");
        internalMap.put("heartbeat-total", "consumer.coordinator.heartbeat.total");
        internalMap.put("join-rate", "consumer.coordinator.join.rate.per.second");
        internalMap.put("join-time-avg", "consumer.coordinator.join.time.ms.avg");
        internalMap.put("join-time-max", "consumer.coordinator.join.time.ms.max");
        internalMap.put("join-total", "consumer.coordinator.join.total");
        internalMap.put("last-heartbeat-seconds-ago", "consumer.coordinator.last.heartbeat.seconds.ago");
        internalMap.put("rebalance-latency-avg", "consumer.coordinator.rebalance.latency.avg");
        internalMap.put("rebalance-latency-max", "consumer.coordinator.rebalance.latency.max");
        internalMap.put("rebalance-latency-total", "consumer.coordinator.rebalance.latency.total");
        internalMap.put("rebalance-rate-per-hour", "consumer.coordinator.rebalance.rate.per.hour");
        internalMap.put("rebalance-total", "consumer.coordinator.rebalance.total");
        internalMap.put("sync-rate", "consumer.coordinator.sync.rate.per.second");
        internalMap.put("sync-time-avg", "consumer.coordinator.sync.time.ms.avg");
        internalMap.put("sync-time-max", "consumer.coordinator.sync.time.ms.max");
        internalMap.put("sync-total", "consumer.coordinator.sync.total");
        kafkaConsumerMetrics.put("consumer-coordinator-metrics", internalMap);

        internalMap = new HashMap<>(19, 1);
        internalMap.put("bytes-consumed-rate", "consumer.fetch.manager.bytes.consumed.rate.per.second");
        internalMap.put("bytes-consumed-total", "consumer.fetch.manager.bytes.consumed.total.bytes");
        internalMap.put("fetch-latency-avg", "consumer.fetch.manager.fetch.latency.ms.avg");
        internalMap.put("fetch-latency-max", "consumer.fetch.manager.fetch.latency.ms.max");
        internalMap.put("fetch-rate", "consumer.fetch.manager.fetch.rate.per.second");
        internalMap.put("fetch-size-avg", "consumer.fetch.manager.fetch.size.bytes.avg");
        internalMap.put("fetch-size-max", "consumer.fetch.manager.fetch.size.bytes.max");
        internalMap.put("fetch-throttle-time-avg", "consumer.fetch.manager.fetch.throttle.time.ms.avg");
        internalMap.put("fetch-throttle-time-max", "consumer.fetch.manager.fetch.throttle.time.ms.max");
        internalMap.put("fetch-total", "consumer.fetch.manager.fetch.total");
        internalMap.put("records-consumed-rate", "consumer.fetch.manager.messages.consumed.rate.per.second");
        internalMap.put("records-consumed-total", "consumer.fetch.manager.messages.consumed.total.per.second");
        internalMap.put("records-lag", "consumer.fetch.manager.messages.lag");
        internalMap.put("records-lag-avg", "consumer.fetch.manager.messages.lag.avg");
        internalMap.put("records-lag-max", "consumer.fetch.manager.messages.lag.max");
        internalMap.put("records-lead", "consumer.fetch.manager.messages.lead");
        internalMap.put("records-lead-avg", "consumer.fetch.manager.messages.lead.avg");
        internalMap.put("records-lead-min", "consumer.fetch.manager.messages.lead.min");
        internalMap.put("records-per-request-avg", "consumer.fetch.manager.messages.per.request.avg");
        kafkaConsumerMetrics.put("consumer-fetch-manager-metrics", internalMap);

        internalMap = new HashMap<>(36, 1);
        internalMap.put("connection-close-rate", "consumer.connection.close.rate.per.second");
        internalMap.put("connection-close-total", "consumer.connection.close.total");
        internalMap.put("connection-count", "consumer.connection.count");
        internalMap.put("connection-creation-rate", "consumer.connection.creation.rate.per.second");
        internalMap.put("connection-creation-total", "consumer.connection.creation.total");
        internalMap.put("failed-authentication-rate", "consumer.failed.authentication.rate.per.second");
        internalMap.put("failed-authentication-total", "consumer.failed.authentication.total");
        internalMap.put("failed-reauthentication-rate", "consumer.failed.reauthentication.rate.per.second");
        internalMap.put("failed-reauthentication-total", "consumer.failed.reauthentication.total");
        internalMap.put("incoming-byte-rate", "consumer.incoming.byte.rate.per.second");
        internalMap.put("incoming-byte-total", "consumer.incoming.byte.total");
        internalMap.put("io-ratio", "consumer.io.ratio");
        internalMap.put("io-time-ns-avg", "consumer.io.time.ns.avg");
        internalMap.put("io-wait-ratio", "consumer.io.wait.ratio");
        internalMap.put("io-wait-time-ns-avg", "consumer.io.wait.time.ns.avg");
        internalMap.put("io-waittime-total", "consumer.io.waittime.total");
        internalMap.put("iotime-total", "consumer.iotime.total");
        internalMap.put("network-io-rate", "consumer.network.io.rate.per.second");
        internalMap.put("network-io-total", "consumer.network.io.total");
        internalMap.put("outgoing-byte-rate", "consumer.outgoing.byte.rate.per.second");
        internalMap.put("outgoing-byte-total", "consumer.outgoing.byte.total");
        internalMap.put("reauthentication-latency-avg", "consumer.reauthentication.latency.ms.avg");
        internalMap.put("reauthentication-latency-max", "consumer.reauthentication.latency.ms.max");
        internalMap.put("request-rate", "consumer.request.rate.per.second");
        internalMap.put("request-size-avg", "consumer.request.size.bytes.avg");
        internalMap.put("request-size-max", "consumer.request.size.bytes.max");
        internalMap.put("request-total", "consumer.request.total");
        internalMap.put("response-rate", "consumer.response.rate.per.second");
        internalMap.put("response-total", "consumer.response.total");
        internalMap.put("select-rate", "consumer.select.rate.per.second");
        internalMap.put("select-total", "consumer.select.total");
        internalMap.put("successful-authentication-no-reauth-total", "consumer.successful.authentication.no.reauth.total");
        internalMap.put("successful-authentication-rate", "consumer.successful.authentication.rate.per.second");
        internalMap.put("successful-authentication-total", "consumer.successful.authentication.total");
        internalMap.put("successful-reauthentication-rate", "consumer.successful.reauthentication.rate.per.second");
        internalMap.put("successful-reauthentication-total", "consumer.successful.reauthentication.total");
        kafkaConsumerMetrics.put("consumer-metrics", internalMap);

        internalMap = new HashMap<>(12, 1);
        internalMap.put("incoming-byte-rate", "consumer.node.incoming.byte.rate.per.second");
        internalMap.put("incoming-byte-total", "consumer.node.incoming.byte.total");
        internalMap.put("outgoing-byte-rate", "consumer.node.outgoing.byte.rate.per.second");
        internalMap.put("outgoing-byte-total", "consumer.node.outgoing.byte.total");
        internalMap.put("request-latency-avg", "consumer.node.request.latency.ms.avg");
        internalMap.put("request-latency-max", "consumer.node.request.latency.ms.max");
        internalMap.put("request-rate", "consumer.node.request.rate.per.second");
        internalMap.put("request-size-avg", "consumer.node.request.size.bytes.avg");
        internalMap.put("request-size-max", "consumer.node.request.size.bytes.max");
        internalMap.put("request-total", "consumer.node.request.total");
        internalMap.put("response-rate", "consumer.node.response.rate.per.second");
        internalMap.put("response-total", "consumer.node.response.total");
        kafkaConsumerMetrics.put("consumer-node-metrics", internalMap);

        // producer
        internalMap = new HashMap<>(65, 1);
        internalMap.put("batch-size-avg", "producer.batch.size.bytes.avg");
        internalMap.put("batch-size-max", "producer.batch.size.bytes.max");
        internalMap.put("batch-split-rate", "producer.batch.split.rate.per.second");
        internalMap.put("batch-split-total", "producer.batch.split.total");
        internalMap.put("buffer-available-bytes", "producer.buffer.available.bytes");
        internalMap.put("buffer-exhausted-rate", "producer.buffer.exhausted.rate.per.second");
        internalMap.put("buffer-exhausted-total", "producer.buffer.exhausted.total");
        internalMap.put("buffer-total-bytes", "producer.buffer.total.bytes");
        internalMap.put("bufferpool-wait-ratio", "producer.bufferpool.wait.ratio.per.second");
        internalMap.put("bufferpool-wait-time-total", "producer.bufferpool.wait.time.total.ms");
        internalMap.put("compression-rate-avg", "producer.compression.rate.avg");
        internalMap.put("connection-close-rate", "producer.connection.close.rate.per.second");
        internalMap.put("connection-close-total", "producer.connection.close.total");
        internalMap.put("connection-count", "producer.connection.count");
        internalMap.put("connection-creation-rate", "producer.connection.creation.rate.per.second");
        internalMap.put("connection-creation-total", "producer.connection.creation.total");
        internalMap.put("failed-authentication-rate", "producer.failed.authentication.rate.per.second");
        internalMap.put("failed-authentication-total", "producer.failed.authentication.total");
        internalMap.put("failed-reauthentication-rate", "producer.failed.reauthentication.rate.per.second");
        internalMap.put("failed-reauthentication-total", "producer.failed.reauthentication.total");
        internalMap.put("incoming-byte-rate", "producer.incoming.byte.rate.per.second");
        internalMap.put("incoming-byte-total", "producer.incoming.byte.total");
        internalMap.put("io-ratio", "producer.io.ratio");
        internalMap.put("io-time-ns-avg", "producer.io.time.ns.avg");
        internalMap.put("io-wait-ratio", "producer.io.wait.ratio");
        internalMap.put("io-wait-time-ns-avg", "producer.io.wait.time.ns.avg");
        internalMap.put("io-waittime-total", "producer.io.waittime.total");
        internalMap.put("iotime-total", "producer.iotime.total");
        internalMap.put("metadata-age", "producer.metadata.age.seconds");
        internalMap.put("network-io-rate", "producer.network.io.rate.per.second");
        internalMap.put("network-io-total", "producer.network.io.total");
        internalMap.put("outgoing-byte-rate", "producer.outgoing.byte.rate.per.second");
        internalMap.put("outgoing-byte-total", "producer.outgoing.byte.total");
        internalMap.put("produce-throttle-time-avg", "producer.produce.throttle.time.ms.avg");
        internalMap.put("produce-throttle-time-max", "producer.produce.throttle.time.ms.max");
        internalMap.put("reauthentication-latency-avg", "producer.reauthentication.latency.ms.avg");
        internalMap.put("reauthentication-latency-max", "producer.reauthentication.latency.ms.max");
        internalMap.put("record-error-rate", "producer.message.error.rate.per.second");
        internalMap.put("record-error-total", "producer.message.error.total");
        internalMap.put("record-queue-time-avg", "producer.message.queue.time.ms.avg");
        internalMap.put("record-queue-time-max", "producer.message.queue.time.ms.max");
        internalMap.put("record-retry-rate", "producer.message.retry.rate.per.second");
        internalMap.put("record-retry-total", "producer.message.retry.total");
        internalMap.put("record-send-rate", "producer.message.send.rate.per.second");
        internalMap.put("record-send-total", "producer.message.send.total");
        internalMap.put("record-size-avg", "producer.message.size.bytes.avg");
        internalMap.put("record-size-max", "producer.message.size.bytes.max");
        internalMap.put("records-per-request-avg", "producer.messages.per.request.avg");
        internalMap.put("request-latency-avg", "producer.request.latency.ms.avg");
        internalMap.put("request-latency-max", "producer.request.latency.ms.max");
        internalMap.put("request-rate", "producer.request.rate.per.second");
        internalMap.put("request-size-avg", "producer.request.size.bytes.avg");
        internalMap.put("request-size-max", "producer.request.size.bytes.max");
        internalMap.put("request-total", "producer.request.total");
        internalMap.put("requests-in-flight", "producer.requests.in.flight");
        internalMap.put("response-rate", "producer.response.rate.per.second");
        internalMap.put("response-total", "producer.response.total");
        internalMap.put("select-rate", "producer.select.rate.per.second");
        internalMap.put("select-total", "producer.select.total");
        internalMap.put("successful-authentication-no-reauth-total", "producer.successful.authentication.no.reauth.total");
        internalMap.put("successful-authentication-rate", "producer.successful.authentication.rate.per.second");
        internalMap.put("successful-authentication-total", "producer.successful.authentication.total");
        internalMap.put("successful-reauthentication-rate", "producer.successful.reauthentication.rate.per.second");
        internalMap.put("successful-reauthentication-total", "producer.successful.reauthentication.total");
        internalMap.put("waiting-threads", "producer.waiting.threads");
        kafkaProducerMetrics.put("producer-metrics", internalMap);

        internalMap = new HashMap<>(12, 1);
        internalMap.put("incoming-byte-rate", "producer.node.incoming.byte.rate.per.second");
        internalMap.put("incoming-byte-total", "producer.node.incoming.byte.total");
        internalMap.put("outgoing-byte-rate", "producer.node.outgoing.byte.rate.per.second");
        internalMap.put("outgoing-byte-total", "producer.node.outgoing.byte.total");
        internalMap.put("request-latency-avg", "producer.node.request.latency.ms.avg");
        internalMap.put("request-latency-max", "producer.node.request.latency.ms.max");
        internalMap.put("request-rate", "producer.node.request.rate.per.second");
        internalMap.put("request-size-avg", "producer.node.request.size.bytes.avg");
        internalMap.put("request-size-max", "producer.node.request.size.bytes.max");
        internalMap.put("request-total", "producer.node.request.total");
        internalMap.put("response-rate", "producer.node.response.rate.per.second");
        internalMap.put("response-total", "producer.node.response.total");
        kafkaProducerMetrics.put("producer-node-metrics", internalMap);
    }

    public static void incrementErrorCounterMetric(
            String topic, int partition, Map<String, KafkaTopicUri> kafkaTopicToTopicUri, String errorMetric, PscConfigurationInternal pscConfigurationInternal
    ) {
        KafkaTopicUri kafkaTopicUri = kafkaTopicToTopicUri.get(topic);
        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                kafkaTopicUri, partition, errorMetric, 1, pscConfigurationInternal);
    }

    public static void handleKafkaClientMetrics(
            Map<MetricName, ? extends Metric> metricsFromKafkaClient,
            Map<String, TopicUri> kafkaTopicToTopicUri,
            boolean isProducer,
            PscConfigurationInternal pscConfigurationInternal
    ) {
        Map<String, Map<String, String>> metricKeysMap = isProducer ? kafkaProducerMetrics : kafkaConsumerMetrics;
        for (Map.Entry<MetricName, ? extends Metric> entry : metricsFromKafkaClient.entrySet()) {
            String name = entry.getKey().name();
            String group = entry.getKey().group();
            if (!metricKeysMap.containsKey(group))
                continue;
            if (!metricKeysMap.get(group).containsKey(name))
                continue;

            String topic = getTopicTagValueFromKafkaClientMetricTags(entry.getKey().tags());
            int partition = getPartitionTagValueFromKafkaClientMetricTags(entry.getKey().tags());
            Object value = entry.getValue().metricValue();
            if (value != null) {
                try {
                    TopicUri topicUri = topic == null ? null : kafkaTopicToTopicUri.get(topic);
                    PscMetricRegistryManager.getInstance().updateBackendHistogramMetric(
                            topicUri,
                            partition,
                            metricKeysMap.get(group).get(name),
                            (long) Double.parseDouble(value.toString()),
                            pscConfigurationInternal
                    );
                } catch (NumberFormatException nfe) {
                    logger.info("Metric reporting failed", nfe);
                }
            }
        }
    }

    private static String getTopicTagValueFromKafkaClientMetricTags(Map<String, String> tags) {
        return tags.getOrDefault("topic", null);
    }

    private static int getPartitionTagValueFromKafkaClientMetricTags(Map<String, String> tags) {
        try {
            return Integer.parseInt(tags.getOrDefault("partition", ""));
        } catch (Exception e) {
            return PscUtils.NO_PARTITION;
        }
    }
}
