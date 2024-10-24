/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.connector.psc.sink;

import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.TopicUriSyntaxException;
import org.apache.flink.api.connector.sink2.Committer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

/**
 * Committer implementation for {@link PscSink}
 *
 * <p>The committer is responsible to finalize the PSC transactions by committing them.
 */
class PscCommitter implements Committer<PscCommittable>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PscCommitter.class);
    public static final String UNKNOWN_PRODUCER_ID_ERROR_MESSAGE =
            "because of a bug in the Kafka broker (KAFKA-9310). Please upgrade to Kafka 2.5+. If you are running with concurrent checkpoints, you also may want to try without them.\n"
                    + "To avoid data loss, the application will restart.";

    private final Properties pscProducerConfig;

    @Nullable private FlinkPscInternalProducer<?, ?> recoveryProducer;

    PscCommitter(Properties pscProducerConfig) {
        this.pscProducerConfig = pscProducerConfig;
    }

    @Override
    public void commit(Collection<CommitRequest<PscCommittable>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<PscCommittable> request : requests) {
            final PscCommittable committable = request.getCommittable();
            final String transactionalId = committable.getTransactionalId();
            LOG.debug("Committing transaction {}", transactionalId);
            Optional<Recyclable<? extends FlinkPscInternalProducer<?, ?>>> recyclable =
                    committable.getProducer();
            FlinkPscInternalProducer<?, ?> producer;
            try {
                producer =
                        recyclable
                                .<FlinkPscInternalProducer<?, ?>>map(Recyclable::getObject)
                                .orElseGet(() -> {
                                    try {
                                        return getRecoveryProducer(committable);
                                    } catch (ConfigurationException | ProducerException | TopicUriSyntaxException e) {
                                        throw new RuntimeException("Error getting recovery producer", e);
                                    }
                                });
                producer.commitTransaction();
                producer.flush();
                recyclable.ifPresent(Recyclable::close);
            } catch (Exception ex) {
                // TODO: make exception handling backend-agnostic
                LOG.warn("Caught exception while committing transaction {}", transactionalId, ex);
                Throwable cause = ex.getCause();
                try {
                    if (cause instanceof Exception)
                        throw (Exception) cause;
                    else
                        throw ex;
                } catch (RetriableException e) {
                    LOG.warn(
                            "Encountered retriable exception while committing {}.", transactionalId, e);
                    request.retryLater();
                } catch (ProducerFencedException e) {
                    // initTransaction has been called on this transaction before
                    LOG.error(
                            "Unable to commit transaction ({}) because its producer is already fenced."
                                    + " This means that you either have a different producer with the same '{}' (this is"
                                    + " unlikely with the '{}' as all generated ids are unique and shouldn't be reused)"
                                    + " or recovery took longer than '{}' ({}ms). In both cases this most likely signals data loss,"
                                    + " please consult the Flink documentation for more details.",
                            request,
                            ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                            PscSink.class.getSimpleName(),
                            ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                            pscProducerConfig.getProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG),
                            e);
                    recyclable.ifPresent(Recyclable::close);
                    request.signalFailedWithKnownReason(e);
                } catch (InvalidTxnStateException e) {
                    // This exception only occurs when aborting after a commit or vice versa.
                    // It does not appear on double commits or double aborts.
                    LOG.error(
                            "Unable to commit transaction ({}) because it's in an invalid state. "
                                    + "Most likely the transaction has been aborted for some reason. Please check the Kafka logs for more details.",
                            request,
                            e);
                    recyclable.ifPresent(Recyclable::close);
                    request.signalFailedWithKnownReason(e);
                } catch (UnknownProducerIdException e) {
                    LOG.error(
                            "Unable to commit transaction ({}) " + UNKNOWN_PRODUCER_ID_ERROR_MESSAGE,
                            request,
                            e);
                    recyclable.ifPresent(Recyclable::close);
                    request.signalFailedWithKnownReason(e);
                } catch (Exception e) {
                    LOG.error(
                            "Transaction ({}) encountered error and data has been potentially lost.",
                            request,
                            e);
                    recyclable.ifPresent(Recyclable::close);
                    request.signalFailedWithUnknownReason(e);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (recoveryProducer != null) {
            recoveryProducer.close();
        }
    }

    /**
     * Creates a producer that can commit into the same transaction as the upstream producer that
     * was serialized into {@link PscCommittable}.
     */
    private FlinkPscInternalProducer<?, ?> getRecoveryProducer(PscCommittable committable) throws ConfigurationException, ProducerException, TopicUriSyntaxException {
        if (recoveryProducer == null) {
            recoveryProducer =
                    new FlinkPscInternalProducer<>(
                            pscProducerConfig, committable.getTransactionalId());
        } else {
            recoveryProducer.setTransactionId(committable.getTransactionalId());
        }
        recoveryProducer.resumeTransaction(committable.getProducerId(), committable.getEpoch());
        return recoveryProducer;
    }
}
