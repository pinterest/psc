package com.pinterest.flink.connector.psc.sink.testutils;

import org.apache.commons.math3.util.Precision;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironmentSettings;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.external.sink.DataStreamSinkExternalContext;
import org.apache.flink.connector.testframe.external.sink.DataStreamSinkV1ExternalContext;
import org.apache.flink.connector.testframe.external.sink.DataStreamSinkV2ExternalContext;
import org.apache.flink.connector.testframe.external.sink.TestingSinkSettings;
import org.apache.flink.connector.testframe.junit.extensions.ConnectorTestingExtension;
import org.apache.flink.connector.testframe.junit.extensions.TestCaseInvocationContextProvider;
import org.apache.flink.connector.testframe.source.FromElementsSource;
import org.apache.flink.connector.testframe.utils.CollectIteratorAssertions;
import org.apache.flink.connector.testframe.utils.ConnectorTestConstants;
import org.apache.flink.connector.testframe.utils.MetricQuerier;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLoggerExtension;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@ExtendWith({ConnectorTestingExtension.class, TestLoggerExtension.class, TestCaseInvocationContextProvider.class})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Experimental
public abstract class PscSinkTestSuiteBase<T extends Comparable<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.connector.testframe.testsuites.SinkTestSuiteBase.class);

    public PscSinkTestSuiteBase() {
    }

    @TestTemplate
    @DisplayName("Test data stream sink")
    public void testBasicSink(TestEnvironment testEnv, DataStreamSinkExternalContext<T> externalContext, CheckpointingMode semantic) throws Exception {
        TestingSinkSettings sinkSettings = this.getTestingSinkSettings(semantic);
        List<T> testRecords = this.generateTestData(sinkSettings, externalContext);
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment(TestEnvironmentSettings.builder().setConnectorJarPaths(externalContext.getConnectorJarPaths()).build());
        execEnv.enableCheckpointing(5000L);
        DataStream<T> dataStream = execEnv.fromCollection(testRecords).name("sourceInSinkTest").setParallelism(1).returns(externalContext.getProducedType());
        this.tryCreateSink(dataStream, externalContext, sinkSettings).setParallelism(1).name("sinkInSinkTest");
        JobClient jobClient = execEnv.executeAsync("DataStream Sink Test");
        CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.FINISHED));
        this.checkResultWithSemantic(externalContext.createSinkDataReader(sinkSettings), testRecords, semantic);
    }

    @TestTemplate
    @DisplayName("Test sink restarting from a savepoint")
    public void testStartFromSavepoint(TestEnvironment testEnv, DataStreamSinkExternalContext<T> externalContext, CheckpointingMode semantic) throws Exception {
        this.restartFromSavepoint(testEnv, externalContext, semantic, 2, 2);
    }

    @TestTemplate
    @DisplayName("Test sink restarting with a higher parallelism")
    public void testScaleUp(TestEnvironment testEnv, DataStreamSinkExternalContext<T> externalContext, CheckpointingMode semantic) throws Exception {
        this.restartFromSavepoint(testEnv, externalContext, semantic, 2, 4);
    }

    @TestTemplate
    @DisplayName("Test sink restarting with a lower parallelism")
    public void testScaleDown(TestEnvironment testEnv, DataStreamSinkExternalContext<T> externalContext, CheckpointingMode semantic) throws Exception {
        this.restartFromSavepoint(testEnv, externalContext, semantic, 4, 2);
    }

    private void restartFromSavepoint(TestEnvironment testEnv, DataStreamSinkExternalContext<T> externalContext, CheckpointingMode semantic, int beforeParallelism, int afterParallelism) throws Exception {
        TestingSinkSettings sinkSettings = this.getTestingSinkSettings(semantic);
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment(TestEnvironmentSettings.builder().setConnectorJarPaths(externalContext.getConnectorJarPaths()).build());
        execEnv.setRestartStrategy(RestartStrategies.noRestart());
        List<T> testRecords = this.generateTestData(sinkSettings, externalContext);
        int numBeforeSuccess = testRecords.size() / 2;
        DataStreamSource<T> source = execEnv.fromSource(new FromElementsSource(Boundedness.CONTINUOUS_UNBOUNDED, testRecords, numBeforeSuccess), WatermarkStrategy.noWatermarks(), "beforeRestartSource").setParallelism(1);
        DataStream<T> dataStream = source.returns(externalContext.getProducedType());
        this.tryCreateSink(dataStream, externalContext, sinkSettings).name("Sink restart test").setParallelism(beforeParallelism);
        CollectResultIterator<T> iterator = this.addCollectSink(source);
        JobClient jobClient = execEnv.executeAsync("Restart Test");
        iterator.setJobClient(jobClient);
        ExecutorService executorService = Executors.newCachedThreadPool();

        String savepointPath;
        try {
            CommonTestUtils.waitForAllTaskRunning(() -> {
                return MetricQuerier.getJobDetails(new RestClient(new Configuration(), executorService), testEnv.getRestEndpoint(), jobClient.getJobID());
            });
            this.waitExpectedSizeData(iterator, numBeforeSuccess);
            savepointPath = (String)jobClient.stopWithSavepoint(true, testEnv.getCheckpointUri(), SavepointFormatType.CANONICAL).get(30L, TimeUnit.SECONDS);
            CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.FINISHED));
        } catch (Exception var25) {
            executorService.shutdown();
            this.killJob(jobClient);
            throw var25;
        }

        List<T> target = testRecords.subList(0, numBeforeSuccess);
        this.checkResultWithSemantic(externalContext.createSinkDataReader(sinkSettings), target, semantic);
        StreamExecutionEnvironment restartEnv = testEnv.createExecutionEnvironment(TestEnvironmentSettings.builder().setConnectorJarPaths(externalContext.getConnectorJarPaths()).setSavepointRestorePath(savepointPath).build());
        restartEnv.enableCheckpointing(5000L);
        DataStreamSource<T> restartSource = restartEnv.fromSource(new FromElementsSource(Boundedness.CONTINUOUS_UNBOUNDED, testRecords, testRecords.size()), WatermarkStrategy.noWatermarks(), "restartSource").setParallelism(1);
        DataStream<T> sinkStream = restartSource.returns(externalContext.getProducedType());
        this.tryCreateSink(sinkStream, externalContext, sinkSettings).setParallelism(afterParallelism);
        this.addCollectSink(restartSource);
        JobClient restartJobClient = restartEnv.executeAsync("Restart Test");

        try {
            this.checkResultWithSemantic(externalContext.createSinkDataReader(sinkSettings), testRecords, semantic);
        } finally {
            executorService.shutdown();
            this.killJob(restartJobClient);
            iterator.close();
        }

    }

    @TestTemplate
    @DisplayName("Test sink metrics")
    public void testMetrics(TestEnvironment testEnv, DataStreamSinkExternalContext<T> externalContext, CheckpointingMode semantic) throws Exception {
        TestingSinkSettings sinkSettings = this.getTestingSinkSettings(semantic);
        int parallelism = 1;
        List<T> testRecords = this.generateTestData(sinkSettings, externalContext);
        String sinkName = "metricTestSink" + testRecords.hashCode();
        StreamExecutionEnvironment env = testEnv.createExecutionEnvironment(TestEnvironmentSettings.builder().setConnectorJarPaths(externalContext.getConnectorJarPaths()).build());
        env.enableCheckpointing(50L);
        DataStreamSource<T> source = env.fromSource(new FromElementsSource(Boundedness.CONTINUOUS_UNBOUNDED, testRecords, testRecords.size()), WatermarkStrategy.noWatermarks(), "metricTestSource").setParallelism(1);
        DataStream<T> dataStream = source.returns(externalContext.getProducedType());
        this.tryCreateSink(dataStream, externalContext, sinkSettings).name(sinkName).setParallelism(parallelism);
        JobClient jobClient = env.executeAsync("Metrics Test");
        MetricQuerier queryRestClient = new MetricQuerier(new Configuration());
        ExecutorService executorService = Executors.newCachedThreadPool();

        try {
            CommonTestUtils.waitForAllTaskRunning(() -> {
                return MetricQuerier.getJobDetails(new RestClient(new Configuration(), executorService), testEnv.getRestEndpoint(), jobClient.getJobID());
            });
            CommonTestUtils.waitUntilCondition(() -> {
                try {
                    return this.compareSinkMetrics(queryRestClient, testEnv, externalContext, jobClient.getJobID(), sinkName, "numRecordsSend", (long)testRecords.size());
                } catch (Exception var8) {
                    return false;
                }
            });
        } finally {
            executorService.shutdown();
            this.killJob(jobClient);
        }

    }

    protected List<T> generateTestData(TestingSinkSettings testingSinkSettings, DataStreamSinkExternalContext<T> externalContext) {
        return externalContext.generateTestData(testingSinkSettings, ThreadLocalRandom.current().nextLong());
    }

    private List<T> pollAndAppendResultData(List<T> result, ExternalSystemDataReader<T> reader, List<T> expected, int retryTimes, CheckpointingMode semantic) {
        long timeoutMs = 1000L;
        int retryIndex = 0;

        while(retryIndex++ < retryTimes && !this.checkGetEnoughRecordsWithSemantic(expected, result, semantic)) {
            result.addAll(reader.poll(Duration.ofMillis(timeoutMs)));
        }

        return result;
    }

    private boolean checkGetEnoughRecordsWithSemantic(List<T> expected, List<T> result, CheckpointingMode semantic) {
        Preconditions.checkNotNull(expected);
        Preconditions.checkNotNull(result);
        if (CheckpointingMode.EXACTLY_ONCE.equals(semantic)) {
            return expected.size() <= result.size();
        } else if (!CheckpointingMode.AT_LEAST_ONCE.equals(semantic)) {
            throw new IllegalStateException(String.format("%s delivery guarantee doesn't support test.", semantic.name()));
        } else {
            Set<Integer> matchedIndex = new HashSet();
            Iterator var5 = expected.iterator();

            int before;
            do {
                if (!var5.hasNext()) {
                    return true;
                }

                T record = (T) var5.next();
                before = matchedIndex.size();

                for(int i = 0; i < result.size(); ++i) {
                    if (!matchedIndex.contains(i) && record.equals(result.get(i))) {
                        matchedIndex.add(i);
                        break;
                    }
                }
            } while(before != matchedIndex.size());

            return false;
        }
    }

    private void checkResultWithSemantic(ExternalSystemDataReader<T> reader, List<T> testData, CheckpointingMode semantic) throws Exception {
        ArrayList<T> result = new ArrayList();
        CommonTestUtils.waitUntilCondition(() -> {
            this.pollAndAppendResultData(result, reader, testData, 30, semantic);

            try {
                CollectIteratorAssertions.assertThat(this.sort(result).iterator()).matchesRecordsFromSource(Arrays.asList(this.sort(testData)), semantic);
                return true;
            } catch (Throwable var6) {
                return false;
            }
        });
    }

    private boolean compareSinkMetrics(MetricQuerier metricQuerier, TestEnvironment testEnv, DataStreamSinkExternalContext<T> context, JobID jobId, String sinkName, String metricsName, long expectedSize) throws Exception {
        double sumNumRecordsOut = metricQuerier.getAggregatedMetricsByRestAPI(testEnv.getRestEndpoint(), jobId, sinkName, metricsName, this.getSinkMetricFilter(context));
        if (Precision.equals((double)expectedSize, sumNumRecordsOut)) {
            return true;
        } else {
            LOG.info("expected:<{}> but was <{}>({})", new Object[]{expectedSize, sumNumRecordsOut, metricsName});
            return false;
        }
    }

    private List<T> sort(List<T> list) {
        return (List)list.stream().sorted().collect(Collectors.toList());
    }

    private TestingSinkSettings getTestingSinkSettings(CheckpointingMode checkpointingMode) {
        return TestingSinkSettings.builder().setCheckpointingMode(checkpointingMode).build();
    }

    private void killJob(JobClient jobClient) throws Exception {
        CommonTestUtils.terminateJob(jobClient);
        CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.CANCELED));
    }

    private DataStreamSink<T> tryCreateSink(DataStream<T> dataStream, DataStreamSinkExternalContext<T> context, TestingSinkSettings sinkSettings) {
        try {
            if (context instanceof DataStreamSinkV1ExternalContext) {
                Sink<T, ?, ?, ?> sinkV1 = ((DataStreamSinkV1ExternalContext)context).createSink(sinkSettings);
                return dataStream.sinkTo(sinkV1);
            } else if (context instanceof DataStreamSinkV2ExternalContext) {
                org.apache.flink.api.connector.sink2.Sink<T> sinkV2 = ((DataStreamSinkV2ExternalContext)context).createSink(sinkSettings);
                return dataStream.sinkTo(sinkV2);
            } else {
                throw new IllegalArgumentException(String.format("The supported context are DataStreamSinkV1ExternalContext and DataStreamSinkV2ExternalContext, but actual is %s.", context.getClass()));
            }
        } catch (UnsupportedOperationException var5) {
            throw new TestAbortedException("Cannot create a sink satisfying given options.", var5);
        }
    }

    private String getSinkMetricFilter(DataStreamSinkExternalContext<T> context) {
        if (context instanceof DataStreamSinkV1ExternalContext) {
            return null;
        } else if (context instanceof DataStreamSinkV2ExternalContext) {
            return "Writer";
        } else {
            throw new IllegalArgumentException(String.format("Get unexpected sink context: %s", context.getClass()));
        }
    }

    protected CollectResultIterator<T> addCollectSink(DataStream<T> stream) {
        TypeSerializer<T> serializer = stream.getType().createSerializer(stream.getExecutionConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<T> factory = new CollectSinkOperatorFactory(serializer, accumulatorName);
        CollectSinkOperator<T> operator = (CollectSinkOperator)factory.getOperator();
        CollectStreamSink<T> sink = new CollectStreamSink(stream, factory);
        sink.name("Data stream collect sink");
        stream.getExecutionEnvironment().addOperator(sink.getTransformation());
        return new CollectResultIterator(operator.getOperatorIdFuture(), serializer, accumulatorName, stream.getExecutionEnvironment().getCheckpointConfig());
    }

    private void waitExpectedSizeData(CollectResultIterator<T> iterator, int targetNum) {
        AssertionsForClassTypes.assertThat(CompletableFuture.supplyAsync(() -> {
            int count;
            for(count = 0; count < targetNum && iterator.hasNext(); ++count) {
                iterator.next();
            }

            if (count < targetNum) {
                throw new IllegalStateException(String.format("Fail to get %d records.", targetNum));
            } else {
                return true;
            }
        })).succeedsWithin(ConnectorTestConstants.DEFAULT_COLLECT_DATA_TIMEOUT);
    }
}

