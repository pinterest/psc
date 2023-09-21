package com.pinterest.psc.interceptor;

import com.pinterest.psc.producer.PscProducerMessage;
import com.pinterest.psc.serde.Serializer;
import com.pinterest.psc.logging.PscLogger;

import java.util.Arrays;
import java.util.List;

/**
 * Control for interceptor flow logic in {@link com.pinterest.psc.producer.PscProducer}
 *
 * The framework allows messages to be processed through a chain of {@link ProducerInterceptor}s before it is
 * sent by the producer. This allows for functionalities such as serialization and metrics emission by default,
 * as well as extensibility to add custom interceptors.
 *
 * In the interceptor flow, a message will first pass through the coreTypedDataInterceptors, which are interceptors
 * that accept the original deserialized type as both the message key and value.
 * Second, it will pass through the necessary {@link SerializerInterceptor}.
 * Finally, it will pass through the coreRawDataInterceptors which will accept
 * the message in its serialized form with {@code byte[]} for both key and value before being sent by the producer.
 *
 * It is necessary to maintain this order of interceptor flow even with custom interceptors added
 * in order to ensure proper production of messages to the backend PubSub.
 */
public class ProducerInterceptors<K, V> {
    private static final PscLogger logger = PscLogger.getLogger(ProducerInterceptors.class);
    private final List<TypePreservingInterceptor<K, V>> coreTypedDataInterceptors;
    private final List<TypePreservingInterceptor<K, V>> configuredTypedDataInterceptors;
    private final SerializerInterceptor<K, V> serializerInterceptor;
    private final List<TypePreservingInterceptor<byte[], byte[]>> configuredRawDataInterceptors;
    private final List<TypePreservingInterceptor<byte[], byte[]>> coreRawDataInterceptors;

    public ProducerInterceptors(
            Interceptors<K, V> config,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer) {
        this.configuredTypedDataInterceptors = config == null ? null : config.getTypedDataInterceptors();
        this.configuredRawDataInterceptors = config == null ? null : config.getRawDataInterceptors();

        this.coreTypedDataInterceptors = Arrays.asList(
                // time lag interceptor
                new TimeLagInterceptor<>().setPscConfigurationInternal(config.getPscConfigurationInternal()),

                // ...

                // metrics reporting interceptor for typed data
                new TypedDataMetricsInterceptor<>().setPscConfigurationInternal(config.getPscConfigurationInternal())
        );

        this.serializerInterceptor = new SerializerInterceptor<>(keySerializer, valueSerializer).setPscConfigurationInternal(config.getPscConfigurationInternal());

        this.coreRawDataInterceptors = Arrays.asList(
                // metrics reporting interceptor for raw data
                new RawDataMetricsInterceptor().setPscConfigurationInternal(config.getPscConfigurationInternal())

                // message corruption detection interceptor
                // auditing interceptor
                // ...
        );
    }

    public final PscProducerMessage<byte[], byte[]> onSend(PscProducerMessage<K, V> message) {
        PscProducerMessage<K, V> typedMessage = message;

        // <custom_typed_data_interceptors>
        if (configuredTypedDataInterceptors != null) {
            for (TypePreservingInterceptor<K, V> interceptor : configuredTypedDataInterceptors) {
                try {
                    typedMessage = interceptor.onSend(typedMessage);
                } catch (Exception e) {
                    logger.warn(
                            "Caught an exception when using the typed interceptor {} on the message {%s:%s}",
                            interceptor.getClass().getName(), typedMessage,
                            e
                    );
                }
            }
        }
        // </custom_typed_data_interceptors>

        // <core_interceptors>
        for (TypePreservingInterceptor<K, V> interceptor : coreTypedDataInterceptors) {
            typedMessage = interceptor.onSend(typedMessage);
        }
        PscProducerMessage<byte[], byte[]> rawMessage = serializerInterceptor.onSend(typedMessage);
        for (TypePreservingInterceptor<byte[], byte[]> interceptor : coreRawDataInterceptors) {
            rawMessage = interceptor.onSend(rawMessage);
        }
        // </core_interceptors>

        // <custom_raw_data_interceptors>
        if (configuredRawDataInterceptors != null) {
            for (TypePreservingInterceptor<byte[], byte[]> interceptor : configuredRawDataInterceptors) {
                try {
                    rawMessage = interceptor.onSend(rawMessage);
                } catch (Exception e) {
                    logger.warn(
                            "Caught an exception when using the raw interceptor {} on the message {%s:%s}",
                            interceptor.getClass().getName(), rawMessage,
                            e
                    );
                }
            }
        }
        // </custom_raw_data_interceptors>

        return rawMessage;
    }
}
