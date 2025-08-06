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

package com.pinterest.flink.streaming.connectors.psc.testutils;

import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import com.pinterest.psc.config.PscConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import java.util.Collection;
import java.util.Properties;
import java.util.Random;

/**
 * Test data generators.
 */
@SuppressWarnings("serial")
public class DataGenerators {

    public static void generateRandomizedIntegerSequence(
            StreamExecutionEnvironment env,
            PscTestEnvironmentWithKafkaAsPubSub testServer,
            String topicUri,
            final int numPartitions,
            final int numElements,
            final boolean randomizeOrder) throws Exception {
        env.setParallelism(numPartitions);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<Integer> stream = env.addSource(
                new RichParallelSourceFunction<Integer>() {

                    private volatile boolean running = true;

                    @Override
                    public void run(SourceContext<Integer> ctx) {
                        // create a sequence
                        int[] elements = new int[numElements];
                        for (int i = 0, val = getRuntimeContext().getIndexOfThisSubtask();
                             i < numElements;
                             i++, val += getRuntimeContext().getNumberOfParallelSubtasks()) {

                            elements[i] = val;
                        }

                        // scramble the sequence
                        if (randomizeOrder) {
                            Random rnd = new Random();
                            for (int i = 0; i < elements.length; i++) {
                                int otherPos = rnd.nextInt(elements.length);

                                int tmp = elements[i];
                                elements[i] = elements[otherPos];
                                elements[otherPos] = tmp;
                            }
                        }

                        // emit the sequence
                        int pos = 0;
                        while (running && pos < elements.length) {
                            ctx.collect(elements[pos++]);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                });

        Properties pscProducerConfiguration = testServer.getStandardPscProducerConfiguration();
        Properties secureConfiguration = testServer.getSecurePscProducerConfiguration();
        if (secureConfiguration != null) {
            pscProducerConfiguration.putAll(secureConfiguration);
        }
        // Ensure the producer enables idempotence.
        pscProducerConfiguration.putAll(testServer.getIdempotentProducerConfig());
        pscProducerConfiguration.putAll(testServer.getPscDiscoveryConfiguration());

        stream = stream.rebalance();
        testServer.produceIntoKafka(stream, topicUri,
                new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, env.getConfig()),
                pscProducerConfiguration,
                new FlinkPscPartitioner<Integer>() {
                    @Override
                    public int partition(Integer next, byte[] serializedKey, byte[] serializedValue, String topicUri, int[] partitions) {
                        return next % partitions.length;
                    }
                });

        env.execute("Scrambles int sequence generator");
    }

    // ------------------------------------------------------------------------

    /**
     * A generator that continuously writes strings into the configured topic. The generation is stopped if an exception
     * occurs or {@link #shutdown()} is called.
     */
    public static class InfiniteStringsGenerator extends Thread {

        private final PscTestEnvironmentWithKafkaAsPubSub pscTestEnvironmentWithKafka;

        private final String topicUri;

        private volatile Throwable error;

        private volatile boolean running = true;

        public InfiniteStringsGenerator(PscTestEnvironmentWithKafkaAsPubSub server, String topicUri) {
            this.pscTestEnvironmentWithKafka = server;
            this.topicUri = topicUri;
        }

        @Override
        public void run() {
            // we manually feed data into the Kafka sink
            OneInputStreamOperatorTestHarness<String, Object> testHarness = null;
            try {
                Properties pscProducerConfiguration = new Properties();
                pscProducerConfiguration.putAll(pscTestEnvironmentWithKafka.getPscDiscoveryConfiguration());
                pscProducerConfiguration.put(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "flink-data-generator-client");
                pscProducerConfiguration.put(PscConfiguration.PSC_METRIC_REPORTING_ENABLED, "false");
                pscProducerConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_RETRIES, "3");

                StreamSink<String> sink = pscTestEnvironmentWithKafka.getProducerSink(
                        topicUri,
                        new SimpleStringSchema(),
                        pscProducerConfiguration,
                        new FlinkFixedPartitioner<>()
                );

                testHarness = new OneInputStreamOperatorTestHarness<>(sink);

                testHarness.open();

                final StringBuilder bld = new StringBuilder();
                final Random rnd = new Random();

                while (running) {
                    bld.setLength(0);

                    int len = rnd.nextInt(100) + 1;
                    for (int i = 0; i < len; i++) {
                        bld.append((char) (rnd.nextInt(20) + 'a'));
                    }

                    String next = bld.toString();
                    testHarness.processElement(new StreamRecord<>(next));
                }
            } catch (Throwable t) {
                this.error = t;
            } finally {
                if (testHarness != null) {
                    try {
                        testHarness.close();
                    } catch (Throwable t) {
                        // ignore
                    }
                }
            }
        }

        public void shutdown() {
            this.running = false;
            this.interrupt();
        }

        public Throwable getError() {
            return this.error;
        }
    }
}
