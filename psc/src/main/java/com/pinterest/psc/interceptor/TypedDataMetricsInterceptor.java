package com.pinterest.psc.interceptor;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetrics;
import com.pinterest.psc.producer.PscProducerMessage;

public class TypedDataMetricsInterceptor<K, V> extends TypePreservingInterceptor<K, V> {
    private static final PscLogger logger = PscLogger.getLogger(TypedDataMetricsInterceptor.class);

    @Override
    public PscProducerMessage<K, V> onSend(PscProducerMessage<K, V> message) {
        TopicUri topicUri = message.getTopicUriPartition().getTopicUri();
        int partition = message.getTopicUriPartition().getPartition();
        K key = message.getKey();
        V value = message.getValue();

        PscMetricRegistryManager.getInstance().incrementCounterMetric(
                topicUri, partition, PscMetrics.PSC_PRODUCER_PRODUCE_MESSAGES_METRIC, pscConfigurationInternal
        );

        if (key != null) {
            PscMetricRegistryManager.getInstance().incrementCounterMetric(
                    topicUri, partition, PscMetrics.PSC_PRODUCER_PRODUCE_KEYED_MESSAGES_METRIC, pscConfigurationInternal
            );
        }

        if (value == null) {
            PscMetricRegistryManager.getInstance().incrementCounterMetric(
                    topicUri, partition, PscMetrics.PSC_PRODUCER_PRODUCE_NULL_VALUE_MESSAGES_METRIC, pscConfigurationInternal
            );
        }

        return super.onSend(message);
    }

    @Override
    public PscConsumerMessage<K, V> onConsume(PscConsumerMessage<K, V> message) {
        MessageId messageId = message.getMessageId();
        TopicUri topicUri = messageId.getTopicUriPartition().getTopicUri();
        K key = message.getKey();
        V value = message.getValue();

        PscMetricRegistryManager.getInstance().incrementCounterMetric(
                topicUri, PscMetrics.PSC_CONSUMER_POLL_MESSAGES_METRIC, pscConfigurationInternal
        );

        if (key != null) {
            PscMetricRegistryManager.getInstance().incrementCounterMetric(
                    topicUri, PscMetrics.PSC_CONSUMER_POLL_KEYED_MESSAGES_METRIC, pscConfigurationInternal
            );
        }

        if (value == null) {
            PscMetricRegistryManager.getInstance().incrementCounterMetric(
                    topicUri, PscMetrics.PSC_CONSUMER_POLL_NULL_MESSAGE_VALUES_METRIC, pscConfigurationInternal
            );
        }

        PscMetricRegistryManager.getInstance().updateHistogramMetric(
                topicUri,
                messageId.getTopicUriPartition().getPartition(),
                PscMetrics.PSC_CONSUMER_OFFSET_MESSAGES_METRIC,
                messageId.getOffset(), pscConfigurationInternal
        );

        PscMetricRegistryManager.getInstance().updateHistogramMetric(
                topicUri,
                PscMetrics.PSC_CONSUMER_POLL_MESSAGE_KEY_SIZE_BYTES_METRIC,
                messageId.getSerializedKeySizeBytes(),
                pscConfigurationInternal
        );

        PscMetricRegistryManager.getInstance().updateHistogramMetric(
                topicUri,
                PscMetrics.PSC_CONSUMER_POLL_MESSAGE_VALUE_SIZE_BYTES_METRIC,
                messageId.getSerializedValueSizeBytes(),
                pscConfigurationInternal
        );

        return message;
    }
}
