package com.pinterest.psc.common.kafka;

import com.google.common.collect.ImmutableMap;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.exception.consumer.BackendConsumerException;
import com.pinterest.psc.exception.consumer.ConsumerException;
import com.pinterest.psc.exception.consumer.DeserializerException;
import com.pinterest.psc.exception.handler.PscErrorHandler;
import com.pinterest.psc.exception.producer.BackendProducerException;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.producer.SerializerException;
import com.pinterest.psc.exception.producer.TransactionalProducerException;
import com.pinterest.psc.logging.PscLogger;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidOffsetException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;

import java.util.ConcurrentModificationException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An evolving class that defines Kafka errors which may be handled by PSC.
 *
 * Exceptions that should be handled by PSC can be added to either consumerExceptionsToHandle or producerExceptionsToHandle.
 * The key for each entry is the exception class to handle, whereas the value of each entry is a {@link LinkedHashMap}
 * with two values - first is a {@link String} that PSC will match with the stack trace of the exception, providing
 * control over handling known cases of the exception's occurrences; second is either a {@link com.pinterest.psc.exception.handler.PscErrorHandler.ConsumerAction}
 * or {@link com.pinterest.psc.exception.handler.PscErrorHandler.ProducerAction} that should be taken.
 */
public class KafkaErrors {
    private static final PscLogger logger = PscLogger.getLogger(KafkaErrors.class);

    // exception class -> (partial exception message -> action)
    // default for partition exception message is empty string (i.e. if none other applies, the default will)
    private static final Map<Class<? extends Exception>, Map<String, PscErrorHandler.ConsumerAction>> consumerExceptionsToHandle =
            ImmutableMap.<Class<? extends Exception>, Map<String, PscErrorHandler.ConsumerAction>> builder()
                    /* Exceptions from Kafka Consumer */

                    // IllegalStateException
                    // example: in org.apache.kafka.clients.ClusterConnectionStates.nodeState ("No entry found for connection")
                    .put(
                            IllegalStateException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put(
                                        "org.apache.kafka.clients.ClusterConnectionStates.nodeState",
                                        new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.RESET_THEN_THROW, ConsumerException.class)
                                );
                            }}
                    )

                    // InterruptException
                    .put(
                            InterruptException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put(
                                        "org.apache.kafka.clients.consumer.KafkaConsumer.close",
                                        new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.NONE, ConsumerException.class)
                                );
                            }}
                    )

                    // InvalidOffsetException
                    .put(
                            InvalidOffsetException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put("", new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.THROW, ConsumerException.class));
                            }}
                    )

                    // NoOffsetForPartitionException
                    .put(
                            NoOffsetForPartitionException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put("", new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.RETRY_THEN_THROW, ConsumerException.class));
                            }}
                    )

                    // NullPointerException
                    .put(
                            NullPointerException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put(
                                        "org.apache.kafka.clients.NetworkClient$DefaultMetadataUpdater.handleCompletedMetadataResponse",
                                        new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.RESET_THEN_THROW, ConsumerException.class)
                                );
                            }}
                    )

                    // OffsetOutOfRangeException
                    .put(
                            OffsetOutOfRangeException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put("", new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.RETRY_THEN_THROW, ConsumerException.class));
                            }}
                    )

                    // SerializationException
                    .put(
                            SerializationException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put("", new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.THROW, DeserializerException.class));
                            }}
                    )

                    // SslAuthenticationException
                    .put(
                            SslAuthenticationException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put("", new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.RESET_THEN_THROW, ConsumerException.class));
                            }}
                    )

                    // TimeoutException
                    // example: in org.apache.kafka.clients.consumer.internals.Fetcher.getTopicMetadata ("Timeout expired while fetching topic metadata")
                    .put(
                            TimeoutException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put(
                                        "org.apache.kafka.clients.consumer.internals.Fetcher.getTopicMetadata",
                                        new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.RETRY_THEN_THROW, ConsumerException.class)
                                );
                            }}
                    )

                    // TopicAuthorizationException
                    .put(
                            TopicAuthorizationException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put("", new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.RETRY_THEN_THROW, ConsumerException.class));
                            }}
                    )

                    // WakeupException
                    .put(
                            WakeupException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put("", new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.THROW, com.pinterest.psc.exception.consumer.WakeupException.class));
                            }}
                    )

                    /* Exceptions from Backend Consumer */

                    // BackendConsumerException
                    .put(
                            BackendConsumerException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put("", new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.THROW, ConsumerException.class));
                            }}
                    )

                    // RetriableCommitFailedException
                    .put(
                            RetriableCommitFailedException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put("", new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.RETRY_RESET_THEN_THROW, ConsumerException.class));
                            }}
                    )

                    // ConcurrentModificationException
                    .put(
                            ConcurrentModificationException.class,
                            new LinkedHashMap<String, PscErrorHandler.ConsumerAction>(1) {{
                                put(
                                        "org.apache.kafka.common.metrics.JmxReporter.getMBeanName", // known case of CME - we will swallow it
                                        new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.NONE, ConsumerException.class)
                                );
                            }}
                    )

                    .build();

    /**
     * Given a consumer exception, return the {@link PscErrorHandler.ConsumerAction} that needs to be taken
     * @param exception
     * @param autoResolutionEnabled
     * @return {@link PscErrorHandler.ConsumerAction} The action that needs to be taken
     */
    public static PscErrorHandler.ConsumerAction shouldHandleConsumerException(Exception exception, boolean autoResolutionEnabled) {
        // default handling
        if (!autoResolutionEnabled || !consumerExceptionsToHandle.containsKey(exception.getClass())) {
            return new PscErrorHandler.ConsumerAction(
                    PscErrorHandler.ActionType.THROW,
                    exception instanceof WakeupException ? com.pinterest.psc.exception.consumer.WakeupException.class : ConsumerException.class
            );
        }

        // when there is a stack trace match
        Map<String, PscErrorHandler.ConsumerAction> exceptionActions = consumerExceptionsToHandle.get(exception.getClass());
        String exceptionStack = PscUtils.getStackTraceAsString(exception);
        for (String key : exceptionActions.keySet()) {
            if (exceptionStack.contains(key)) {
                logger.info("Found follow up action for stack trace element: [{}]", key);
                return exceptionActions.get(key);
            }
        }

        // otherwise
        return exceptionActions.getOrDefault(
                "",
                new PscErrorHandler.ConsumerAction(PscErrorHandler.ActionType.THROW, ConsumerException.class)
        );
    }

    // exception class -> (partial exception message -> action)
    // default for partition exception message is empty string (i.e. if none other applies, the default will)
    private static final Map<Class<? extends Exception>, Map<String, PscErrorHandler.ProducerAction>> producerExceptionsToHandle =
            ImmutableMap.<Class<? extends Exception>, Map<String, PscErrorHandler.ProducerAction>> builder()
                    /* Exceptions from Kafka Producer */

                    // IllegalStateException
                    // example: in org.apache.kafka.clients.ClusterConnectionStates.nodeState ("No entry found for connection")
                    .put(
                            IllegalStateException.class,
                            new LinkedHashMap<String, PscErrorHandler.ProducerAction>(1) {{
                                put(
                                        "org.apache.kafka.clients.ClusterConnectionStates.nodeState",
                                        new PscErrorHandler.ProducerAction(PscErrorHandler.ActionType.RESET_THEN_THROW, ProducerException.class)
                                );
                            }}
                    )

                    // InterruptException
                    .put(
                            InterruptException.class,
                            new LinkedHashMap<String, PscErrorHandler.ProducerAction>(1) {{
                                put(
                                        "org.apache.kafka.clients.producer.KafkaProducer.close",
                                        new PscErrorHandler.ProducerAction(PscErrorHandler.ActionType.NONE, ProducerException.class)
                                );
                            }}
                    )

                    // NotLeaderForPartitionException
                    .put(
                            NotLeaderForPartitionException.class,
                            new LinkedHashMap<String, PscErrorHandler.ProducerAction>(1) {{
                                put("", new PscErrorHandler.ProducerAction(PscErrorHandler.ActionType.RESET_THEN_THROW, ProducerException.class));
                            }}
                    )

                    // ProducerFencedException
                    .put(
                            ProducerFencedException.class,
                            new LinkedHashMap<String, PscErrorHandler.ProducerAction>(1) {{
                                put("", new PscErrorHandler.ProducerAction(PscErrorHandler.ActionType.THROW, TransactionalProducerException.class));
                            }}
                    )

                    // SerializationException
                    .put(
                            SerializationException.class,
                            new LinkedHashMap<String, PscErrorHandler.ProducerAction>(1) {{
                                put("", new PscErrorHandler.ProducerAction(PscErrorHandler.ActionType.THROW, SerializerException.class));
                            }}
                    )

                    // TimeoutException
                    // example: in org.apache.kafka.clients.producer.KafkaProducer.waitOnMetadata ("Topic %s not present in metadata after %d ms.")
                    .put(
                            TimeoutException.class,
                            new LinkedHashMap<String, PscErrorHandler.ProducerAction>(2) {{
                                put(
                                        "org.apache.kafka.clients.producer.KafkaProducer.waitOnMetadata",
                                        new PscErrorHandler.ProducerAction(PscErrorHandler.ActionType.RETRY_THEN_THROW, ProducerException.class)
                                );
                                put(
                                        "not present in metadata after",
                                        new PscErrorHandler.ProducerAction(PscErrorHandler.ActionType.RESET_THEN_THROW, ProducerException.class)
                                );
                                put(
                                        "has passed since batch creation",
                                        new PscErrorHandler.ProducerAction(PscErrorHandler.ActionType.RESET_THEN_THROW, ProducerException.class)
                                );
                            }}
                    )

                    // TopicAuthorizationException
                    .put(
                            TopicAuthorizationException.class,
                            new LinkedHashMap<String, PscErrorHandler.ProducerAction>(1) {{
                                put("", new PscErrorHandler.ProducerAction(PscErrorHandler.ActionType.RETRY_THEN_THROW, ProducerException.class));
                            }}
                    )

                    /* Exceptions from Backend Producer */

                    // BackendProducerException
                    .put(
                            BackendProducerException.class,
                            new LinkedHashMap<String, PscErrorHandler.ProducerAction>(1) {{
                                put("", new PscErrorHandler.ProducerAction(PscErrorHandler.ActionType.THROW, ProducerException.class));
                            }}
                    )

                    .build();

    /**
     * Given a producer exception, return the {@link PscErrorHandler.ProducerAction} that needs to be taken
     * @param exception
     * @param autoResolutionEnabled
     * @return {@link PscErrorHandler.ProducerAction} The action that needs to be taken
     */
    public static PscErrorHandler.ProducerAction shouldHandleProducerException(Exception exception, boolean autoResolutionEnabled) {
        // default handling
        if (!autoResolutionEnabled || !producerExceptionsToHandle.containsKey(exception.getClass()))
            return new PscErrorHandler.ProducerAction(PscErrorHandler.ActionType.THROW, ProducerException.class);

        // when there is a stack trace match
        Map<String, PscErrorHandler.ProducerAction> exceptionActions = producerExceptionsToHandle.get(exception.getClass());
        String exceptionStack = PscUtils.getStackTraceAsString(exception);
        for (String key : exceptionActions.keySet()) {
            if (exceptionStack.contains(key)) {
                logger.info("Found follow up action for stack trace element: [{}]", key);
                return exceptionActions.get(key);
            }
        }

        // otherwise
        return exceptionActions.getOrDefault(
                "",
                new PscErrorHandler.ProducerAction(PscErrorHandler.ActionType.THROW, ProducerException.class)
        );
    }
}
