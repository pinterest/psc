package com.pinterest.psc.interceptor;

import com.pinterest.psc.common.MessageId;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.consumer.PscConsumerMessage;
import com.pinterest.psc.logging.PscLogger;
import com.pinterest.psc.serde.Deserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Control for interceptor flow logic in {@link com.pinterest.psc.consumer.PscConsumer}
 *
 * The framework allows messages to be processed through a chain of {@link ConsumerInterceptor}s before it is
 * returned by {@link com.pinterest.psc.consumer.ToPscMessageIteratorConverter#next()}. This allows for functionalities
 * such as deserialization and metrics emission by default, as well as extensibility to add custom interceptors.
 *
 * In the interceptor flow, a message will first pass through the coreRawDataInterceptors, which are interceptors
 * that accept byte arrays {@code byte[]} as both the message key and value. Second, it will pass through the necessary
 * {@link DeserializerInterceptor}. Finally, it will pass through the coreTypedDataInterceptors which will accept
 * the message in its deserialized form before being returned by
 * {@link com.pinterest.psc.consumer.ToPscMessageIteratorConverter#next()}.
 *
 * It is necessary to maintain this order of interceptor flow even with custom interceptors added
 * in order to ensure proper consumption of messages.
 */
public class ConsumerInterceptors<K, V> {
    private static final PscLogger logger = PscLogger.getLogger(ConsumerInterceptors.class);
    private final List<TypePreservingInterceptor<byte[], byte[]>> coreRawDataInterceptors;
    private final List<TypePreservingInterceptor<byte[], byte[]>> configuredRawDataInterceptors;
    private final DeserializerInterceptor<K, V> deserializerInterceptor;
    private final List<TypePreservingInterceptor<K, V>> configuredTypedDataInterceptors;
    private final List<TypePreservingInterceptor<K, V>> coreTypedDataInterceptors;

    public ConsumerInterceptors(
            Interceptors<K, V> config,
            PscConfigurationInternal pscConfigurationInternal,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer
    ) {
        this.configuredRawDataInterceptors = config == null ? null : config.getRawDataInterceptors();
        this.configuredTypedDataInterceptors = config == null ? null : config.getTypedDataInterceptors();

        // default raw data interceptors should go here
        this.coreRawDataInterceptors = Arrays.asList(
                // metrics reporting interceptor for raw data
                new RawDataMetricsInterceptor().setPscConfigurationInternal(pscConfigurationInternal)
                // message corruption detection interceptor
                // auditing interceptor
                // ...

                // chargeback logging interceptor
                // chargebackInterceptor
        );

        this.deserializerInterceptor = new DeserializerInterceptor<>(keyDeserializer, valueDeserializer).setPscConfigurationInternal(pscConfigurationInternal);

        // default typed data interceptors should go here
        this.coreTypedDataInterceptors = Arrays.asList(
                // time lag interceptor
                new TimeLagInterceptor<>().setPscConfigurationInternal(pscConfigurationInternal),

                // ...

                // metrics reporting interceptor for typed data
                new TypedDataMetricsInterceptor<>().setPscConfigurationInternal(pscConfigurationInternal)
        );
    }

    public final PscConsumerMessage<K, V> onConsume(PscConsumerMessage<byte[], byte[]> message) {
        PscConsumerMessage<byte[], byte[]> rawMessage = message;

        // <custom_serialized_data_interceptors>
        if (configuredRawDataInterceptors != null) {
            for (TypePreservingInterceptor<byte[], byte[]> interceptor : configuredRawDataInterceptors) {
                try {
                    rawMessage = interceptor.onConsume(rawMessage);
                } catch (Exception e) {
                    logger.warn(
                            "Caught an exception when using the raw interceptor {} on the message with id {}",
                            interceptor.getClass().getName(), rawMessage.getMessageId(),
                            e
                    );
                }
            }
        }
        // </custom_serialized_data_interceptors>

        // <core_interceptors>
        for (TypePreservingInterceptor<byte[], byte[]> interceptor : coreRawDataInterceptors) {
            rawMessage = interceptor.onConsume(rawMessage);
        }
        PscConsumerMessage<K, V> typedMessage = deserializerInterceptor.onConsume(rawMessage);
        for (TypePreservingInterceptor<K, V> interceptor : coreTypedDataInterceptors) {
            typedMessage = interceptor.onConsume(typedMessage);
        }
        // </core_interceptors>

        // <custom_deserialized_data_interceptors>
        if (configuredTypedDataInterceptors != null) {
            for (TypePreservingInterceptor<K, V> interceptor : configuredTypedDataInterceptors) {
                try {
                    typedMessage = interceptor.onConsume(typedMessage);
                } catch (Exception e) {
                    logger.warn(
                            "Caught an exception when using the typed interceptor {} on the message with id {}",
                            interceptor.getClass().getName(), typedMessage.getMessageId(),
                            e
                    );
                }
            }
        }
        // </custom_deserialized_data_interceptors>

        return typedMessage;
    }

    public void onCommit(Collection<MessageId> messageIds) {
        // <custom_serialized_data_interceptors>
        if (configuredRawDataInterceptors != null)
            onCommit(messageIds, configuredRawDataInterceptors);
        // </custom_serialized_data_interceptors>

        // <core_interceptors>
        onCommit(messageIds, coreRawDataInterceptors);
        onCommit(messageIds, deserializerInterceptor);
        onCommit(messageIds, coreTypedDataInterceptors);
        // </core_interceptors>

        // <custom_deserialized_data_interceptors>
        if (configuredTypedDataInterceptors != null)
            onCommit(messageIds, configuredTypedDataInterceptors);
        // </custom_deserialized_data_interceptors>
    }

    private <K1, V1> void onCommit(
            Collection<MessageId> messageIds,
            List<TypePreservingInterceptor<K1, V1>> consumerInterceptors
    ) {
        consumerInterceptors.forEach(consumerInterceptor -> onCommit(messageIds, consumerInterceptor));
    }

    private void onCommit(
            Collection<MessageId> messageIds,
            ConsumerInterceptor consumerInterceptor
    ) {
        try {
            consumerInterceptor.onCommit(messageIds);
        } catch (Exception e) {
            // do not propagate interceptor exception, just log
            logger.warn("Error executing interceptor onCommit callback", e);
        }
    }

}
