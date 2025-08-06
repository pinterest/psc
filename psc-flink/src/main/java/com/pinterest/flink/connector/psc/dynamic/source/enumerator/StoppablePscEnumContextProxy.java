/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.connector.psc.dynamic.source.enumerator;

import com.pinterest.flink.connector.psc.dynamic.metadata.PscMetadataService;
import com.pinterest.flink.connector.psc.dynamic.source.split.DynamicPscSourceSplit;
import com.pinterest.flink.connector.psc.source.split.PscTopicUriPartitionSplit;
import com.pinterest.psc.exception.PscException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * A proxy enumerator context that supports life cycle management of underlying threads related to a
 * sub {@link com.pinterest.flink.connector.psc.source.enumerator.PscSourceEnumerator}. This is
 * motivated by the need to cancel the periodic partition discovery in scheduled tasks when sub
 * PSC Enumerators are restarted. The worker thread pool in {@link
 * org.apache.flink.runtime.source.coordinator.SourceCoordinatorContext} should not contain tasks of
 * inactive PscSourceEnumerators, after source restart.
 *
 * <p>Due to the inability to cancel scheduled tasks from {@link
 * org.apache.flink.runtime.source.coordinator.SourceCoordinatorContext}, this enumerator context
 * will safely catch exceptions during enumerator restart and use a closeable proxy scheduler to
 * invoke tasks on the coordinator main thread to maintain the single threaded property.
 */
@Internal
public class StoppablePscEnumContextProxy
        implements SplitEnumeratorContext<PscTopicUriPartitionSplit>, AutoCloseable {
    private static final Logger logger =
            LoggerFactory.getLogger(StoppablePscEnumContextProxy.class);

    private final String clusterId;
    private final PscMetadataService pscMetadataService;
    private final SplitEnumeratorContext<DynamicPscSourceSplit> enumContext;
    private final ScheduledExecutorService subEnumeratorWorker;
    private final Runnable signalNoMoreSplitsCallback;
    private boolean noMoreSplits = false;
    private volatile boolean isClosing;

    /**
     * Constructor for the enumerator context.
     *
     * @param clusterId The cluster id in order to maintain the mapping to the sub
     *     PscSourceEnumerator
     * @param pscMetadataService the PSC metadata service to facilitate error handling
     * @param enumContext the underlying enumerator context
     * @param signalNoMoreSplitsCallback the callback when signal no more splits is invoked
     */
    public StoppablePscEnumContextProxy(
            String clusterId,
            PscMetadataService pscMetadataService,
            SplitEnumeratorContext<DynamicPscSourceSplit> enumContext,
            @Nullable Runnable signalNoMoreSplitsCallback) {
        this.clusterId = clusterId;
        this.pscMetadataService = pscMetadataService;
        this.enumContext = enumContext;
        this.subEnumeratorWorker =
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory(clusterId + "-enum-worker"));
        this.signalNoMoreSplitsCallback = signalNoMoreSplitsCallback;
        this.isClosing = false;
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
        return enumContext.metricGroup();
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        enumContext.sendEventToSourceReader(subtaskId, event);
    }

    @Override
    public int currentParallelism() {
        return enumContext.currentParallelism();
    }

    @Override
    public Map<Integer, ReaderInfo> registeredReaders() {
        return enumContext.registeredReaders();
    }

    /** Wrap splits with cluster metadata. */
    @Override
    public void assignSplits(SplitsAssignment<PscTopicUriPartitionSplit> newSplitAssignments) {
        if (logger.isInfoEnabled()) {
            logger.info(
                    "Assigning {} splits for cluster {}: {}",
                    newSplitAssignments.assignment().values().stream()
                            .mapToLong(Collection::size)
                            .sum(),
                    clusterId,
                    newSplitAssignments);
        }

        Map<Integer, List<DynamicPscSourceSplit>> readerToSplitsMap = new HashMap<>();
        newSplitAssignments
                .assignment()
                .forEach(
                        (subtask, splits) ->
                                readerToSplitsMap.put(
                                        subtask,
                                        splits.stream()
                                                .map(
                                                        split ->
                                                                new DynamicPscSourceSplit(
                                                                        clusterId, split))
                                                .collect(Collectors.toList())));

        if (!readerToSplitsMap.isEmpty()) {
            enumContext.assignSplits(new SplitsAssignment<>(readerToSplitsMap));
        }
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
        // There are no more splits for this cluster, but we need to wait until all clusters are
        // finished with their respective split discoveries. In the PSC Source, this is called in
        // the coordinator thread, ensuring thread safety, for all source readers at the same time.
        noMoreSplits = true;
        if (signalNoMoreSplitsCallback != null) {
            // Thread safe idempotent callback
            signalNoMoreSplitsCallback.run();
        }
    }

    /** Execute the one time callables in the coordinator. */
    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        enumContext.callAsync(
                wrapCallAsyncCallable(callable), wrapCallAsyncCallableHandler(handler));
    }

    /**
     * Schedule task via internal thread pool to proxy task so that the task handler callback can
     * execute in the single threaded source coordinator thread pool to avoid synchronization needs.
     *
     * <p>Having the scheduled task in the internal thread pool also allows us to cancel the task
     * when the context needs to close due to dynamic enumerator restart.
     *
     * <p>In the case of PscEnumerator partition discovery, the callback modifies PscEnumerator
     * object state.
     */
    @Override
    public <T> void callAsync(
            Callable<T> callable,
            BiConsumer<T, Throwable> handler,
            long initialDelay,
            long period) {
        subEnumeratorWorker.scheduleAtFixedRate(
                () -> callAsync(callable, handler), initialDelay, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
        enumContext.runInCoordinatorThread(runnable);
    }

    public boolean isNoMoreSplits() {
        return noMoreSplits;
    }

    /**
     * Note that we can't close the source coordinator here, because these contexts can be closed
     * during metadata change when the coordinator still needs to continue to run. We can only close
     * the coordinator context in Flink job shutdown, which Flink will do for us. That's why there
     * is the complexity of the internal thread pools in this class.
     *
     * <p>TODO: Attach Flink JIRA ticket -- discuss with upstream how to cancel scheduled tasks
     * belonging to enumerator.
     */
    @Override
    public void close() throws Exception {
        logger.info("Closing enum context for {}", clusterId);
        if (subEnumeratorWorker != null) {
            // PscSubscriber worker thread will fail if admin client is closed in the middle.
            // Swallow the error and set the context to closed state.
            isClosing = true;
            subEnumeratorWorker.shutdown();
            subEnumeratorWorker.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Wraps callable in call async executed in worker thread pool with exception propagation to
     * optimize on doing IO in non-coordinator thread.
     */
    protected <T> Callable<T> wrapCallAsyncCallable(Callable<T> callable) {
        return () -> {
            try {
                return callable.call();
            } catch (Exception e) {
                if (isClosing) {
                    throw new HandledFlinkPscException(e, clusterId);
                }

                Optional<PscException> throwable =
                        ExceptionUtils.findThrowable(e, PscException.class);
                if (throwable.isPresent()
                        && !pscMetadataService.isClusterActive(clusterId)) {
                    throw new HandledFlinkPscException(throwable.get(), clusterId);
                }

                throw e;
            }
        };
    }

    /**
     * Handle exception that is propagated by a callable, executed on coordinator thread. Depending
     * on condition(s) the exception may be swallowed or forwarded. This is the topic
     * partition discovery callable handler.
     */
    protected <T> BiConsumer<T, Throwable> wrapCallAsyncCallableHandler(
            BiConsumer<T, Throwable> mainHandler) {
        return (result, t) -> {
            // check if exception is handled
            Optional<HandledFlinkPscException> throwable =
                    ExceptionUtils.findThrowable(t, HandledFlinkPscException.class);
            if (throwable.isPresent()) {
                logger.warn("Swallowed handled exception for {}.", clusterId, throwable.get());
                return;
            }

            // let the main handler deal with the potential exception
            mainHandler.accept(result, t);
        };
    }

    /**
     * General exception to signal to internal exception handling mechanisms that a benign error
     * occurred.
     */
    @Internal
    public static class HandledFlinkPscException extends RuntimeException {
        private static final String ERROR_MESSAGE = "An error occurred with %s";

        private final String clusterId;

        public HandledFlinkPscException(Throwable cause, String clusterId) {
            super(cause);
            this.clusterId = clusterId;
        }

        public String getMessage() {
            return String.format(ERROR_MESSAGE, clusterId);
        }
    }

    /**
     * This factory exposes a way to override the {@link StoppablePscEnumContextProxy} used in the
     * enumerator. This pluggable factory is extended in unit tests to facilitate invoking the
     * periodic discovery loops on demand.
     */
    @Internal
    public interface StoppablePscEnumContextProxyFactory {

        StoppablePscEnumContextProxy create(
                SplitEnumeratorContext<DynamicPscSourceSplit> enumContext,
                String clusterId,
                PscMetadataService metadataService,
                Runnable signalNoMoreSplitsCallback);

        static StoppablePscEnumContextProxyFactory getDefaultFactory() {
            return (enumContext,
                    clusterId,
                    metadataService,
                    signalNoMoreSplitsCallback) ->
                    new StoppablePscEnumContextProxy(
                            clusterId,
                            metadataService,
                            enumContext,
                            signalNoMoreSplitsCallback);
        }
    }
}
