package com.pinterest.psc.interceptor;

import com.pinterest.psc.common.PscCommon;
import com.pinterest.psc.common.PscMessage;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.metrics.PscMetricRegistryManager;
import com.pinterest.psc.metrics.PscMetrics;
import com.pinterest.psc.producer.PscProducerMessage;

import java.util.Map;

public class TimeLagInterceptor<K, V> extends TypePreservingInterceptor<K, V> {
    private static final PscLogger logger = PscLogger.getLogger(TimeLagInterceptor.class);

    @Override
    public PscConsumerMessage<K, V> onConsume(PscConsumerMessage<K, V> message) {
        Map<String, byte[]> headers = message.getHeaders();
        TopicUri topicUri = message.getMessageId().getTopicUriPartition().getTopicUri();
        int partition = message.getMessageId().getTopicUriPartition().getPartition();
        if (headers == null || !headers.containsKey(PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP)) {
            // header does not have relevant information, use message publish timestamp instead
            long lag = getLagInMilliseconds(message.getPublishTimestamp());
            PscMetricRegistryManager.getInstance().updateHistogramMetric(
                    topicUri, partition, PscMetrics.PSC_CONSUMER_TIME_LAG_MS_METRIC, lag, pscConfigurationInternal
            );
            message.addTag(PscConsumerMessage.DefaultPscConsumerMessageTags.HEADER_TIMESTAMP_NOT_FOUND);
            return message;
        }

        // otherwise use PSC-injected header
        byte[] timestampBytes = headers.get(PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP);
        if (timestampBytes.length != Long.BYTES) {
            // byte buffer length is not Long.BYTES
            logger.warn("Timestamp header length is not Long.BYTES (8) from message " + message.getMessageId());
            message.addTag(PscConsumerMessage.DefaultPscConsumerMessageTags.HEADER_TIMESTAMP_CORRUPTED);
            return message;
        }

        long lag = getLagInMilliseconds(PscCommon.byteArrayToLong(timestampBytes));
        PscMetricRegistryManager.getInstance().updateHistogramMetric(
                topicUri, partition, PscMetrics.PSC_CONSUMER_TIME_LAG_MS_METRIC, lag, pscConfigurationInternal
        );

        return super.onConsume(message);
    }

    @Override
    public PscProducerMessage<K, V> onSend(PscProducerMessage<K, V> message) {
        message.setHeader(
                PscMessage.PSC_MESSAGE_HEADER_PSC_PRODUCE_TIMESTAMP,
                PscCommon.longToByteArray(System.currentTimeMillis())
        );
        return super.onSend(message);
    }

    private long getLagInMilliseconds(long timestamp) {
        long currentTsMs = System.currentTimeMillis();
        long lag = currentTsMs - timestamp;
        if (lag < 0) {
            // try microseconds
            /*
            Instant instant = Instant.now();
            long currentTsUs = TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + instant.getNano() / 1_000;
            lag = currentTsUs - timestamp;
             */
            lag = currentTsMs * 1_000 - timestamp;
            if (lag < 0) {
                // try nanoseconds as last resort
                /*
                long currentTsNs = TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
                return (currentTsNs - timestamp) / 1_000_000;
                 */
                return (currentTsMs * 1_000_000 - timestamp) / 1_000_000;
            }
            return lag / 1_000;
        }
        return lag;
    }
}
