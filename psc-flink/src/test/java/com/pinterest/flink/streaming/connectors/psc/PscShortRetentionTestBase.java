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

package com.pinterest.flink.streaming.connectors.psc;

import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.consumer.PscConsumerMessage;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.InstantiationUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;

import static org.apache.flink.test.util.TestUtils.tryExecute;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A class containing a special Kafka broker which has a log retention of only 250 ms.
 * This way, we can make sure our consumer is properly handling cases where we run into out of offset
 * errors
 */
@SuppressWarnings("serial")
public class PscShortRetentionTestBase implements Serializable {

    protected static final Logger LOG = LoggerFactory.getLogger(PscShortRetentionTestBase.class);

    protected static final int NUM_TMS = 1;

    protected static final int TM_SLOTS = 8;

    protected static final int PARALLELISM = NUM_TMS * TM_SLOTS;

    private static PscTestEnvironmentWithKafkaAsPubSub pscTestEnvWithKafka;
    private static Properties standardProps;

    @ClassRule
    public static MiniClusterWithClientResource flink = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setConfiguration(getConfiguration())
                    .setNumberTaskManagers(NUM_TMS)
                    .setNumberSlotsPerTaskManager(TM_SLOTS)
                    .build());

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    protected static Properties secureProps = new Properties();

    private static Configuration getConfiguration() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("16m"));
        return flinkConfig;
    }

    @BeforeClass
    public static void prepare() throws Exception {
        LOG.info("-------------------------------------------------------------------------");
        LOG.info("    Starting KafkaShortRetentionTestBase ");
        LOG.info("-------------------------------------------------------------------------");

        // dynamically load the implementation for the test
        Class<?> clazz = Class.forName("org.apache.flink.streaming.connectors.kafka.KafkaTestEnvironmentImpl");
        pscTestEnvWithKafka = (PscTestEnvironmentWithKafkaAsPubSub) InstantiationUtil.instantiate(clazz);

        LOG.info("Starting KafkaTestBase.prepare() for Kafka " + pscTestEnvWithKafka.getVersion());

        if (pscTestEnvWithKafka.isSecureRunSupported()) {
            secureProps.putAll(pscTestEnvWithKafka.getSecureKafkaConfiguration());
        }

        Properties specificProperties = new Properties();
        specificProperties.setProperty("log.retention.hours", "0");
        specificProperties.setProperty("log.retention.minutes", "0");
        specificProperties.setProperty("log.retention.ms", "250");
        specificProperties.setProperty("log.retention.check.interval.ms", "100");
        pscTestEnvWithKafka.prepare(pscTestEnvWithKafka.createConfig().setKafkaServerProperties(specificProperties));

        standardProps = pscTestEnvWithKafka.getStandardKafkaProperties();
    }

    @AfterClass
    public static void shutDownServices() throws Exception {
        pscTestEnvWithKafka.shutdown();

        secureProps.clear();
    }

    /**
     * This test is concurrently reading and writing from a kafka topic.
     * The job will run for a while
     * In a special deserializationSchema, we make sure that the offsets from the topic
     * are non-continuous (because the data is expiring faster than its consumed --> with auto.offset.reset = 'earliest', some offsets will not show up)
     */
    private static boolean stopProducer = false;

    public void runAutoOffsetResetTest() throws Exception {
        final String topic = "auto-offset-reset-test";
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + topic;

        final int parallelism = 1;
        final int elementsPerPartition = 50000;

        Properties tprops = new Properties();
        tprops.setProperty("retention.ms", "250");
        pscTestEnvWithKafka.createTestTopic(topic, parallelism, 1, tprops);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setRestartStrategy(RestartStrategies.noRestart()); // fail immediately

        // ----------- add producer dataflow ----------

        DataStream<String> stream = env.addSource(new RichParallelSourceFunction<String>() {

            private boolean running = true;

            @Override
            public void run(SourceContext<String> ctx) throws InterruptedException {
                int cnt = getRuntimeContext().getIndexOfThisSubtask() * elementsPerPartition;
                int limit = cnt + elementsPerPartition;

                while (running && !stopProducer && cnt < limit) {
                    ctx.collect("element-" + cnt);
                    cnt++;
                    Thread.sleep(10);
                }
                LOG.info("Stopping producer");
            }

            @Override
            public void cancel() {
                running = false;
            }
        });
        Properties producerProps = new Properties();
        producerProps.putAll(pscTestEnvWithKafka.getStandardPscProducerConfiguration());
        producerProps.putAll(pscTestEnvWithKafka.getSecurePscProducerConfiguration());
        producerProps.putAll(pscTestEnvWithKafka.getPscDiscoveryConfiguration());
        pscTestEnvWithKafka.produceIntoKafka(stream, topicUri, new SimpleStringSchema(), producerProps, null);

        // ----------- add consumer dataflow ----------

        Properties consumerProps = new Properties();
        consumerProps.putAll(pscTestEnvWithKafka.getStandardPscConsumerConfiguration());
        consumerProps.putAll(pscTestEnvWithKafka.getSecurePscConsumerConfiguration());
        consumerProps.putAll(pscTestEnvWithKafka.getPscDiscoveryConfiguration());
        NonContinousOffsetsDeserializationSchema deserSchema = new NonContinousOffsetsDeserializationSchema();
        FlinkPscConsumerBase<String> source = pscTestEnvWithKafka.getPscConsumer(topicUri, deserSchema, consumerProps);

        DataStreamSource<String> consuming = env.addSource(source);
        consuming.addSink(new DiscardingSink<String>());

        tryExecute(env, "run auto offset reset test");

        pscTestEnvWithKafka.deleteTestTopic(topic);
    }

    private class NonContinousOffsetsDeserializationSchema implements PscDeserializationSchema<String> {
        private int numJumps;
        long nextExpected = 0;

        @Override
        public String deserialize(PscConsumerMessage<byte[], byte[]> record) {
            final long offset = record.getMessageId().getOffset();
            if (offset != nextExpected) {
                numJumps++;
                nextExpected = offset;
                LOG.info("Registered now jump at offset {}", offset);
            }
            nextExpected++;
            try {
                Thread.sleep(10); // slow down data consumption to trigger log eviction
            } catch (InterruptedException e) {
                throw new RuntimeException("Stopping it");
            }
            return "";
        }

        @Override
        public boolean isEndOfStream(String nextElement) {
            if (numJumps >= 5) {
                // we saw 5 jumps and no failures --> consumer can handle auto.offset.reset
                stopProducer = true;
                return true;
            }
            return false;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    /**
     * Ensure that the consumer is properly failing if "auto.offset.reset" is set to "none".
     */
    public void runFailOnAutoOffsetResetNone() throws Exception {
        final String topic = "auto-offset-reset-none-test";
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + topic;
        final int parallelism = 1;

        pscTestEnvWithKafka.createTestTopic(topic, parallelism, 1);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setRestartStrategy(RestartStrategies.noRestart()); // fail immediately

        // ----------- add consumer ----------

        Properties customProps = new Properties();
        customProps.putAll(pscTestEnvWithKafka.getStandardPscConsumerConfiguration());
        customProps.putAll(pscTestEnvWithKafka.getSecurePscConsumerConfiguration());
        customProps.putAll(pscTestEnvWithKafka.getPscDiscoveryConfiguration());
        customProps.setProperty(
                PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET,
                PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_NONE
        ); // test that "none" leads to an exception
        FlinkPscConsumerBase<String> source = pscTestEnvWithKafka.getPscConsumer(topicUri, new SimpleStringSchema(), customProps);

        DataStreamSource<String> consuming = env.addSource(source);
        consuming.addSink(new DiscardingSink<String>());

        try {
            env.execute("Test auto offset reset none");
        } catch (Throwable e) {
            // check if correct exception has been thrown
            if (!e.getCause().getCause().getMessage().contains("Undefined offset with no reset policy for partition")) {
                throw e;
            }
        }

        pscTestEnvWithKafka.deleteTestTopic(topic);
    }

    public void runFailOnAutoOffsetResetNoneEager() throws Exception {
        final String topic = "auto-offset-reset-none-test";
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_TOPIC_URI_PREFIX + topic;
        final int parallelism = 1;

        pscTestEnvWithKafka.createTestTopic(topic, parallelism, 1);

        // ----------- add consumer ----------

        Properties customProps = new Properties();
        customProps.putAll(pscTestEnvWithKafka.getStandardPscConsumerConfiguration());
        customProps.putAll(pscTestEnvWithKafka.getSecurePscConsumerConfiguration());
        customProps.putAll(pscTestEnvWithKafka.getPscDiscoveryConfiguration());
        customProps.setProperty(
                PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET,
                PscConfiguration.PSC_CONSUMER_OFFSET_AUTO_RESET_NONE
        ); // test that "none" leads to an exception

        try {
            pscTestEnvWithKafka.getPscConsumer(topicUri, new SimpleStringSchema(), customProps);
            fail("should fail with an exception");
        } catch (IllegalArgumentException e) {
            // expected
            assertTrue(e.getMessage().contains("none"));
        }

        pscTestEnvWithKafka.deleteTestTopic(topic);
    }
}
