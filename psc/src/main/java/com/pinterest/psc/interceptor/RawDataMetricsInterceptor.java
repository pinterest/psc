package com.pinterest.psc.interceptor;

import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.PscMessage;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetrics;
import com.pinterest.psc.producer.PscProducerMessage;

public class RawDataMetricsInterceptor extends TypePreservingInterceptor<byte[], byte[]> {
    private static final PscLogger logger = PscLogger.getLogger(RawDataMetricsInterceptor.class);

    @Override
    public PscProducerMessage<byte[], byte[]> onSend(PscProducerMessage<byte[], byte[]> message) {
        TopicUri topicUri = message.getTopicUriPartition().getTopicUri();
        int partition = message.getTopicUriPartition().getPartition();
        byte[] key = message.getKey();
        byte[] value = message.getValue();

        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                topicUri, partition, PscMetrics.PSC_PRODUCER_PRODUCE_MESSAGES_METRIC, pscConfigurationInternal
        );

        if (key != null) {
            PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                    topicUri, partition, PscMetrics.PSC_PRODUCER_PRODUCE_KEYED_MESSAGES_METRIC, pscConfigurationInternal
            );
        }

        if (value == null) {
            PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                    topicUri, partition, PscMetrics.PSC_PRODUCER_PRODUCE_NULL_VALUE_MESSAGES_METRIC, pscConfigurationInternal
            );
        }

        PscMetricRegistryManager.getInstance().updateBackendHistogramMetric(
                topicUri,
                partition,
                PscMetrics.PSC_PRODUCER_PRODUCE_MESSAGE_KEY_SIZE_BYTES_METRIC,
                key == null ? -1 : key.length, pscConfigurationInternal
        );
        message.setHeader(
                PscMessage.PSC_MESSAGE_HEADER_KEY_SIZE_BYTES,
                PscCommon.intToByteArray(key == null ? -1 : key.length)
        );

        PscMetricRegistryManager.getInstance().updateBackendHistogramMetric(
                topicUri,
                partition,
                PscMetrics.PSC_PRODUCER_PRODUCE_MESSAGE_VALUE_SIZE_BYTES_METRIC,
                value == null ? -1 : value.length, pscConfigurationInternal
        );
        message.setHeader(
                PscMessage.PSC_MESSAGE_HEADER_VALUE_SIZE_BYTES,
                PscCommon.intToByteArray(value == null ? -1 : value.length)
        );

        return super.onSend(message);
    }

    @Override
    public PscConsumerMessage<byte[], byte[]> onConsume(PscConsumerMessage<byte[], byte[]> message) {
        TopicUri topicUri = message.getMessageId().getTopicUriPartition().getTopicUri();
        int partition = message.getMessageId().getTopicUriPartition().getPartition();
        byte[] key = message.getKey();
        byte[] value = message.getValue();

        PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                topicUri, partition, PscMetrics.PSC_CONSUMER_POLL_MESSAGES_METRIC, pscConfigurationInternal
        );

        if (key != null) {
            PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                    topicUri, partition, PscMetrics.PSC_CONSUMER_POLL_KEYED_MESSAGES_METRIC, pscConfigurationInternal
            );
        }

        if (value == null) {
            PscMetricRegistryManager.getInstance().incrementBackendCounterMetric(
                    topicUri, partition, PscMetrics.PSC_CONSUMER_POLL_NULL_MESSAGE_VALUES_METRIC, pscConfigurationInternal
            );
        }

        PscMetricRegistryManager.getInstance().updateBackendHistogramMetric(
                topicUri,
                partition,
                PscMetrics.PSC_CONSUMER_POLL_MESSAGE_KEY_SIZE_BYTES_METRIC,
                key == null ? -1 : key.length, pscConfigurationInternal
        );

        PscMetricRegistryManager.getInstance().updateBackendHistogramMetric(
                topicUri,
                partition,
                PscMetrics.PSC_CONSUMER_POLL_MESSAGE_VALUE_SIZE_BYTES_METRIC,
                value == null ? -1 : value.length, pscConfigurationInternal
        );

        return super.onConsume(message);
    }
}
