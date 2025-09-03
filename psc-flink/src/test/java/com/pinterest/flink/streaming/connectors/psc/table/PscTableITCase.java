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

package com.pinterest.flink.streaming.connectors.psc.table;

import com.pinterest.flink.streaming.connectors.psc.PscTestEnvironmentWithKafkaAsPubSub;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import com.pinterest.psc.config.PscConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.pinterest.flink.streaming.connectors.psc.table.PscTableTestUtils.collectAllRows;
import static com.pinterest.flink.streaming.connectors.psc.table.PscTableTestUtils.collectRows;
import static com.pinterest.flink.streaming.connectors.psc.table.PscTableTestUtils.readLines;
import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT;
import static org.apache.flink.table.utils.TableTestMatchers.deepEqualTo;
import static org.apache.flink.util.CollectionUtil.entry;
import static org.apache.flink.util.CollectionUtil.map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Basic IT cases for the Kafka table source and sink. */
@RunWith(Parameterized.class)
public class PscTableITCase extends PscTableTestBase {

    private static final String JSON_FORMAT = "json";
    private static final String AVRO_FORMAT = "avro";
    private static final String CSV_FORMAT = "csv";

    @Parameterized.Parameter public String format;

    @Parameterized.Parameters(name = "format = {0}")
    public static Collection<String> parameters() {
        return Arrays.asList(JSON_FORMAT, AVRO_FORMAT, CSV_FORMAT);
    }

    @Before
    public void before() {
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);
    }

    @Test
    public void testPscSourceSink() throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "tstopic_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into PSC -------------------
        String groupId = getStandardProps().getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "create table kafka (\n"
                                + "  `computed-price` as price + 1.0,\n"
                                + "  price decimal(38, 18),\n"
                                + "  currency string,\n"
                                + "  log_date date,\n"
                                + "  log_time time(3),\n"
                                + "  log_ts timestamp(3),\n"
                                + "  ts as log_ts + INTERVAL '1' SECOND,\n"
                                + "  watermark for ts as ts\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        groupId,
                        formatOptions());

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "SELECT CAST(price AS DECIMAL(10, 2)), currency, "
                        + " CAST(d AS DATE), CAST(t AS TIME(0)), CAST(ts AS TIMESTAMP(3))\n"
                        + "FROM (VALUES (2.02,'Euro','2019-12-12', '00:00:01', '2019-12-12 00:00:01.001001'), \n"
                        + "  (1.11,'US Dollar','2019-12-12', '00:00:02', '2019-12-12 00:00:02.002001'), \n"
                        + "  (50,'Yen','2019-12-12', '00:00:03', '2019-12-12 00:00:03.004001'), \n"
                        + "  (3.1,'Euro','2019-12-12', '00:00:04', '2019-12-12 00:00:04.005001'), \n"
                        + "  (5.33,'US Dollar','2019-12-12', '00:00:05', '2019-12-12 00:00:05.006001'), \n"
                        + "  (0,'DUMMY','2019-12-12', '00:00:10', '2019-12-12 00:00:10'))\n"
                        + "  AS orders (price, currency, d, t, ts)";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        String query =
                "SELECT\n"
                        + "  CAST(TUMBLE_END(ts, INTERVAL '5' SECOND) AS VARCHAR),\n"
                        + "  CAST(MAX(log_date) AS VARCHAR),\n"
                        + "  CAST(MAX(log_time) AS VARCHAR),\n"
                        + "  CAST(MAX(ts) AS VARCHAR),\n"
                        + "  COUNT(*),\n"
                        + "  CAST(MAX(price) AS DECIMAL(10, 2))\n"
                        + "FROM kafka\n"
                        + "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(2);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("Job_2");
        } catch (Throwable e) {
            // we have to use a specific exception to indicate the job is finished,
            // because the registered Kafka source is infinite.
            if (!isCausedByJobFinished(e)) {
                // re-throw
                throw e;
            }
        }

        List<String> expected =
                Arrays.asList(
                        "+I(2019-12-12 00:00:05.000,2019-12-12,00:00:03,2019-12-12 00:00:04.004,3,50.00)",
                        "+I(2019-12-12 00:00:10.000,2019-12-12,00:00:05,2019-12-12 00:00:06.006,2,5.33)");

        assertThat(TestingSinkFunction.rows).isEqualTo(expected);

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }

    @Test
    public void testPscSourceSinkWithBoundedSpecificOffsets() throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "bounded_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `user_id` INT,\n"
                                + "  `item_id` INT,\n"
                                + "  `behavior` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'scan.bounded.mode' = 'specific-offsets',\n"
                                + "  'scan.bounded.specific-offsets' = 'partition:0,offset:2',\n"
                                + "  %s\n"
                                + ")\n",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        groupId,
                        formatOptions());

        tEnv.executeSql(createTable);

        List<Row> values =
                Arrays.asList(
                        Row.of(1, 1102, "behavior 1"),
                        Row.of(2, 1103, "behavior 2"),
                        Row.of(3, 1104, "behavior 3"));
        tEnv.fromValues(values).insertInto("kafka").execute().await();

        // ---------- Consume stream from Kafka -------------------

        List<Row> results = collectAllRows(tEnv.sqlQuery("SELECT * from kafka"));

        assertThat(results)
                .containsExactly(Row.of(1, 1102, "behavior 1"), Row.of(2, 1103, "behavior 2"));

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }

    @Test
    public void testPscSourceSinkWithBoundedTimestamp() throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "bounded_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty("group.id");
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `user_id` INT,\n"
                                + "  `item_id` INT,\n"
                                + "  `behavior` STRING,\n"
                                + "  `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'scan.bounded.mode' = 'timestamp',\n"
                                + "  'scan.bounded.timestamp-millis' = '5',\n"
                                + "  %s\n"
                                + ")\n",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        groupId,
                        formatOptions());

        tEnv.executeSql(createTable);

        List<Row> values =
                Arrays.asList(
                        Row.of(1, 1102, "behavior 1", Instant.ofEpochMilli(0L)),
                        Row.of(2, 1103, "behavior 2", Instant.ofEpochMilli(3L)),
                        Row.of(3, 1104, "behavior 3", Instant.ofEpochMilli(7L)));
        tEnv.fromValues(values).insertInto("kafka").execute().await();

        // ---------- Consume stream from Kafka -------------------

        List<Row> results = collectAllRows(tEnv.sqlQuery("SELECT * from kafka"));

        assertThat(results)
                .containsExactly(
                        Row.of(1, 1102, "behavior 1", Instant.ofEpochMilli(0L)),
                        Row.of(2, 1103, "behavior 2", Instant.ofEpochMilli(3L)));

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }

    @Test
    public void testPscTableWithMultipleTopics() throws Exception {
        // ---------- create source and sink tables -------------------
        String tableTemp =
                "create table %s (\n"
                        + "  currency string\n"
                        + ") with (\n"
                        + "  'connector' = '%s',\n"
                        + "  'topic-uri' = '%s',\n"
                        + "  'properties.psc.cluster.uri' = '%s',\n"
                        + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                        + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                        + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                        + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                        + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                        + "  'properties.psc.consumer.group.id' = '%s',\n"
                        + "  'scan.startup.mode' = 'earliest-offset',\n"
                        + "  %s\n"
                        + ")";
        String groupId = getStandardProps().getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String bootstraps = getBootstrapServers();
        List<String> currencies = Arrays.asList("Euro", "Dollar", "Yen", "Dummy");
        List<String> topics =
                currencies.stream()
                        .map(
                                currency ->
                                        String.format(
                                                "%s_%s_%s", currency, format, UUID.randomUUID()))
                        .collect(Collectors.toList());
        List<String> topicUris = topics.stream().map(t -> PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + t).collect(Collectors.toList());
        // Because psc connector currently doesn't support write data into multiple topic
        // together,
        // we have to create multiple sink tables.
        IntStream.range(0, 4)
                .forEach(
                        index -> {
                            createTestTopic(topics.get(index), 1, 1);
                            tEnv.executeSql(
                                    String.format(
                                            tableTemp,
                                            currencies.get(index).toLowerCase(),
                                            PscDynamicTableFactory.IDENTIFIER,
                                            topicUris.get(index),
                                            PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                                            PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                                            bootstraps,
                                            groupId,
                                            formatOptions()));
                        });
        // create source table
        tEnv.executeSql(
                String.format(
                        tableTemp,
                        "currencies_topic_list",
                        PscDynamicTableFactory.IDENTIFIER,
                        String.join(";", topicUris),
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        groupId,
                        formatOptions()));

        // ---------- Prepare data in Kafka topics -------------------
        String insertTemp =
                "INSERT INTO %s\n"
                        + "SELECT currency\n"
                        + " FROM (VALUES ('%s'))\n"
                        + " AS orders (currency)";
        currencies.forEach(
                currency -> {
                    try {
                        tEnv.executeSql(String.format(insertTemp, currency.toLowerCase(), currency))
                                .await();
                    } catch (Exception e) {
                        fail(e.getMessage());
                    }
                });

        // ------------- test the topic-list kafka source -------------------
        DataStream<RowData> result =
                tEnv.toAppendStream(
                        tEnv.sqlQuery("SELECT currency FROM currencies_topic_list"), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(4); // expect to receive 4 records
        result.addSink(sink);

        try {
            env.execute("Job_3");
        } catch (Throwable e) {
            // we have to use a specific exception to indicate the job is finished,
            // because the registered Kafka source is infinite.
            if (!isCausedByJobFinished(e)) {
                // re-throw
                throw e;
            }
        }
        List<String> expected = Arrays.asList("+I(Dollar)", "+I(Dummy)", "+I(Euro)", "+I(Yen)");
        TestingSinkFunction.rows.sort(Comparator.naturalOrder());
        assertThat(TestingSinkFunction.rows).isEqualTo(expected);

        // ------------- cleanup -------------------
        topics.forEach(super::deleteTestTopic);
    }

    @Test
    public void testPscSourceSinkWithMetadata() throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "metadata_topic_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `physical_1` STRING,\n"
                                + "  `physical_2` INT,\n"
                                // metadata fields are out of order on purpose
                                // offset is ignored because it might not be deterministic
//                                + "  `timestamp-type` STRING METADATA VIRTUAL,\n"
                                + "  `timestamp` TIMESTAMP(3) METADATA,\n"
//                                + "  `leader-epoch` INT METADATA VIRTUAL,\n"
                                + "  `headers` MAP<STRING, BYTES> METADATA,\n"
                                + "  `partition` INT METADATA VIRTUAL,\n"
                                + "  `topic-uri` STRING METADATA VIRTUAL,\n"
                                + "  `physical_3` BOOLEAN\n"
                                + ") WITH (\n"
                                + "  'connector' = 'psc',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        topicUri, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, bootstraps, groupId, formatOptions());
        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " ('data 1', 1, TIMESTAMP '2020-03-08 13:12:11.123', MAP['k1', X'C0FFEE', 'k2', X'BABE01'], TRUE),\n"
                        + " ('data 2', 2, TIMESTAMP '2020-03-09 13:12:11.123', CAST(NULL AS MAP<STRING, BYTES>), FALSE),\n"
                        + " ('data 3', 3, TIMESTAMP '2020-03-10 13:12:11.123', MAP['k1', X'102030', 'k2', X'203040'], TRUE)";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM kafka"), 3);

        final List<Row> expected =
                Arrays.asList(
                        Row.of(
                                "data 1",
                                1,
                                LocalDateTime.parse("2020-03-08T13:12:11.123"),
                                map(
                                        entry("k1", EncodingUtils.decodeHex("C0FFEE")),
                                        entry("k2", EncodingUtils.decodeHex("BABE01"))),
                                0,
                                topicUri,
                                true),
                        Row.of(
                                "data 2",
                                2,
                                LocalDateTime.parse("2020-03-09T13:12:11.123"),
                                Collections.emptyMap(),
                                0,
                                topicUri,
                                false),
                        Row.of(
                                "data 3",
                                3,
                                LocalDateTime.parse("2020-03-10T13:12:11.123"),
                                map(
                                        entry("k1", EncodingUtils.decodeHex("102030")),
                                        entry("k2", EncodingUtils.decodeHex("203040"))),
                                0,
                                topicUri,
                                true));

        assertThat(result).satisfies(matching(deepEqualTo(expected, true)));

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }

    @Test
    public void testPscSourceSinkWithKeyAndPartialValue() throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "key_partial_value_topic_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String bootstraps = getBootstrapServers();

        // k_user_id and user_id have different data types to verify the correct mapping,
        // fields are reordered on purpose
        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `k_user_id` BIGINT,\n"
                                + "  `name` STRING,\n"
                                + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                                + "  `k_event_id` BIGINT,\n"
                                + "  `user_id` INT,\n"
                                + "  `payload` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'psc',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'key.format' = '%s',\n"
                                + "  'key.fields' = 'k_event_id; k_user_id',\n"
                                + "  'key.fields-prefix' = 'k_',\n"
                                + "  'value.format' = '%s',\n"
                                + "  'value.fields-include' = 'EXCEPT_KEY'\n"
                                + ")",
                        topicUri, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, bootstraps, groupId, format, format);

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " (1, 'name 1', TIMESTAMP '2020-03-08 13:12:11.123', 100, 41, 'payload 1'),\n"
                        + " (2, 'name 2', TIMESTAMP '2020-03-09 13:12:11.123', 101, 42, 'payload 2'),\n"
                        + " (3, 'name 3', TIMESTAMP '2020-03-10 13:12:11.123', 102, 43, 'payload 3')";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM kafka"), 3);

        final List<Row> expected =
                Arrays.asList(
                        Row.of(
                                1L,
                                "name 1",
                                LocalDateTime.parse("2020-03-08T13:12:11.123"),
                                100L,
                                41,
                                "payload 1"),
                        Row.of(
                                2L,
                                "name 2",
                                LocalDateTime.parse("2020-03-09T13:12:11.123"),
                                101L,
                                42,
                                "payload 2"),
                        Row.of(
                                3L,
                                "name 3",
                                LocalDateTime.parse("2020-03-10T13:12:11.123"),
                                102L,
                                43,
                                "payload 3"));

        assertThat(result).satisfies(matching(deepEqualTo(expected, true)));

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }

    @Test
    public void testPscSourceSinkWithKeyAndFullValue() throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "key_full_value_topic_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String bootstraps = getBootstrapServers();

        // compared to the partial value test we cannot support both k_user_id and user_id in a full
        // value due to duplicate names after key prefix stripping,
        // fields are reordered on purpose,
        // fields for keys and values are overlapping
        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `user_id` BIGINT,\n"
                                + "  `name` STRING,\n"
                                + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                                + "  `event_id` BIGINT,\n"
                                + "  `payload` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'psc',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'key.format' = '%s',\n"
                                + "  'key.fields' = 'event_id; user_id',\n"
                                + "  'value.format' = '%s',\n"
                                + "  'value.fields-include' = 'ALL'\n"
                                + ")",
                        topicUri, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, bootstraps, groupId, format, format);

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " (1, 'name 1', TIMESTAMP '2020-03-08 13:12:11.123', 100, 'payload 1'),\n"
                        + " (2, 'name 2', TIMESTAMP '2020-03-09 13:12:11.123', 101, 'payload 2'),\n"
                        + " (3, 'name 3', TIMESTAMP '2020-03-10 13:12:11.123', 102, 'payload 3')";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM kafka"), 3);

        final List<Row> expected =
                Arrays.asList(
                        Row.of(
                                1L,
                                "name 1",
                                LocalDateTime.parse("2020-03-08T13:12:11.123"),
                                100L,
                                "payload 1"),
                        Row.of(
                                2L,
                                "name 2",
                                LocalDateTime.parse("2020-03-09T13:12:11.123"),
                                101L,
                                "payload 2"),
                        Row.of(
                                3L,
                                "name 3",
                                LocalDateTime.parse("2020-03-10T13:12:11.123"),
                                102L,
                                "payload 3"));

        assertThat(result).satisfies(matching(deepEqualTo(expected, true)));

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }

    @Test
    public void testPscTemporalJoinChangelog() throws Exception {
        // Set the session time zone to UTC, because the next `METADATA FROM
        // 'value.source.timestamp'` DDL
        // will use the session time zone when convert the changelog time from milliseconds to
        // timestamp
        tEnv.getConfig().set(TableConfigOptions.LOCAL_TIME_ZONE, "UTC");

        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String orderTopic = "temporal_join_topic_order_" + format + "_" + UUID.randomUUID();
        final String orderTopicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + orderTopic;
        createTestTopic(orderTopic, 1, 1);

        final String productTopic =
                "temporal_join_topic_product_" + format + "_" + UUID.randomUUID();
        final String productTopicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + productTopic;
        createTestTopic(productTopic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String bootstraps = getBootstrapServers();

        // create order table and set initial values
        final String orderTableDDL =
                String.format(
                        "CREATE TABLE ordersTable (\n"
                                + "  order_id STRING,\n"
                                + "  product_id STRING,\n"
                                + "  order_time TIMESTAMP(3),\n"
                                + "  quantity INT,\n"
                                + "  purchaser STRING,\n"
                                + "  WATERMARK FOR order_time AS order_time - INTERVAL '1' SECOND\n"
                                + ") WITH (\n"
                                + "  'connector' = 'psc',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'format' = '%s'\n"
                                + ")",
                        orderTopicUri, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, bootstraps, groupId, format);
        tEnv.executeSql(orderTableDDL);
        String orderInitialValues =
                "INSERT INTO ordersTable\n"
                        + "VALUES\n"
                        + "('o_001', 'p_001', TIMESTAMP '2020-10-01 00:01:00', 1, 'Alice'),"
                        + "('o_002', 'p_002', TIMESTAMP '2020-10-01 00:02:00', 1, 'Bob'),"
                        + "('o_003', 'p_001', TIMESTAMP '2020-10-01 12:00:00', 2, 'Tom'),"
                        + "('o_004', 'p_002', TIMESTAMP '2020-10-01 12:00:00', 2, 'King'),"
                        + "('o_005', 'p_001', TIMESTAMP '2020-10-01 18:00:00', 10, 'Leonard'),"
                        + "('o_006', 'p_002', TIMESTAMP '2020-10-01 18:00:00', 10, 'Leonard'),"
                        + "('o_007', 'p_002', TIMESTAMP '2020-10-01 18:00:01', 10, 'Robinson')"; // used to advance watermark
        tEnv.executeSql(orderInitialValues).await();

        // create product table and set initial values
        final String productTableDDL =
                String.format(
                        "CREATE TABLE productChangelogTable (\n"
                                + "  product_id STRING,\n"
                                + "  product_name STRING,\n"
                                + "  product_price DECIMAL(10, 4),\n"
                                + "  update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,\n"
                                + "  PRIMARY KEY(product_id) NOT ENFORCED,\n"
                                + "  WATERMARK FOR update_time AS update_time - INTERVAL '1' SECOND\n"
                                + ") WITH (\n"
                                + "  'connector' = 'psc',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'value.format' = 'debezium-json'\n"
                                + ")",
                        productTopicUri, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, bootstraps, groupId);
        tEnv.executeSql(productTableDDL);

        // use raw format to initial the changelog data
        initialProductChangelog(productTopicUri, bootstraps);

        // ---------- query temporal join result from Kafka -------------------
        final List<String> result =
                collectRows(
                                tEnv.sqlQuery(
                                        "SELECT"
                                                + "  order_id,"
                                                + "  order_time,"
                                                + "  P.product_id,"
                                                + "  P.update_time as product_update_time,"
                                                + "  product_price,"
                                                + "  purchaser,"
                                                + "  product_name,"
                                                + "  quantity,"
                                                + "  quantity * product_price AS order_amount "
                                                + "FROM ordersTable AS O "
                                                + "LEFT JOIN productChangelogTable FOR SYSTEM_TIME AS OF O.order_time AS P "
                                                + "ON O.product_id = P.product_id"),
                                6)
                        .stream()
                        .map(row -> row.toString())
                        .sorted()
                        .collect(Collectors.toList());

        final List<String> expected =
                Arrays.asList(
                        "+I[o_001, 2020-10-01T00:01, p_001, 1970-01-01T00:00, 11.1100, Alice, scooter, 1, 11.1100]",
                        "+I[o_002, 2020-10-01T00:02, p_002, 1970-01-01T00:00, 23.1100, Bob, basketball, 1, 23.1100]",
                        "+I[o_003, 2020-10-01T12:00, p_001, 2020-10-01T12:00, 12.9900, Tom, scooter, 2, 25.9800]",
                        "+I[o_004, 2020-10-01T12:00, p_002, 2020-10-01T12:00, 19.9900, King, basketball, 2, 39.9800]",
                        "+I[o_005, 2020-10-01T18:00, p_001, 2020-10-01T18:00, 11.9900, Leonard, scooter, 10, 119.9000]",
                        "+I[o_006, 2020-10-01T18:00, null, null, null, Leonard, null, 10, null]");

        assertThat(result).isEqualTo(expected);

        // ------------- cleanup -------------------

        deleteTestTopic(orderTopic);
        deleteTestTopic(productTopic);
    }

    private void initialProductChangelog(String topic, String bootstraps) throws Exception {
        String productChangelogDDL =
                String.format(
                        "CREATE TABLE productChangelog (\n"
                                + "  changelog STRING"
                                + ") WITH (\n"
                                + "  'connector' = 'psc',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.consumer.group.id' = 'psc-test-group-id',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'format' = 'raw'\n"
                                + ")",
                        topic, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, bootstraps);
        tEnv.executeSql(productChangelogDDL);
        String[] allChangelog = readLines("product_changelog.txt").toArray(new String[0]);

        StringBuilder insertSqlSb = new StringBuilder();
        insertSqlSb.append("INSERT INTO productChangelog VALUES ");
        for (String log : allChangelog) {
            insertSqlSb.append("('" + log + "'),");
        }
        // trim the last comma
        String insertSql = insertSqlSb.substring(0, insertSqlSb.toString().length() - 1);
        tEnv.executeSql(insertSql).await();
    }

    @Test
    public void testPerPartitionWatermarkPsc() throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "per_partition_watermark_topic_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 4, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `partition_id` INT,\n"
                                + "  `name` STRING,\n"
                                + "  `timestamp` TIMESTAMP(3),\n"
                                + "  WATERMARK FOR `timestamp` AS `timestamp`\n"
                                + ") WITH (\n"
                                + "  'connector' = 'psc',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'sink.partitioner' = '%s',\n"
                                + "  'format' = '%s'\n"
                                + ")",
                        topicUri, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, bootstraps, groupId, TestPartitioner.class.getName(), format);

        tEnv.executeSql(createTable);

        // make every partition have more than one record
        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " (0, 'partition-0-name-0', TIMESTAMP '2020-03-08 13:12:11.123'),\n"
                        + " (0, 'partition-0-name-1', TIMESTAMP '2020-03-08 14:12:12.223'),\n"
                        + " (0, 'partition-0-name-2', TIMESTAMP '2020-03-08 15:12:13.323'),\n"
                        + " (1, 'partition-1-name-0', TIMESTAMP '2020-03-09 13:13:11.123'),\n"
                        + " (1, 'partition-1-name-1', TIMESTAMP '2020-03-09 15:13:11.133'),\n"
                        + " (1, 'partition-1-name-2', TIMESTAMP '2020-03-09 16:13:11.143'),\n"
                        + " (2, 'partition-2-name-0', TIMESTAMP '2020-03-10 13:12:14.123'),\n"
                        + " (2, 'partition-2-name-1', TIMESTAMP '2020-03-10 14:12:14.123'),\n"
                        + " (2, 'partition-2-name-2', TIMESTAMP '2020-03-10 14:13:14.123'),\n"
                        + " (2, 'partition-2-name-3', TIMESTAMP '2020-03-10 14:14:14.123'),\n"
                        + " (2, 'partition-2-name-4', TIMESTAMP '2020-03-10 14:15:14.123'),\n"
                        + " (2, 'partition-2-name-5', TIMESTAMP '2020-03-10 14:16:14.123'),\n"
                        + " (3, 'partition-3-name-0', TIMESTAMP '2020-03-11 17:12:11.123'),\n"
                        + " (3, 'partition-3-name-1', TIMESTAMP '2020-03-11 18:12:11.123')";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        env.setParallelism(1);
        String createSink =
                "CREATE TABLE MySink(\n"
                        + "  id INT,\n"
                        + "  name STRING,\n"
                        + "  ts TIMESTAMP(3),\n"
                        + "  WATERMARK FOR ts as ts\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink.drop-late-event' = 'true'\n"
                        + ")";
        tEnv.executeSql(createSink);
        TableResult tableResult = tEnv.executeSql("INSERT INTO MySink SELECT * FROM kafka");
        final List<String> expected =
                Arrays.asList(
                        "+I[0, partition-0-name-0, 2020-03-08T13:12:11.123]",
                        "+I[0, partition-0-name-1, 2020-03-08T14:12:12.223]",
                        "+I[0, partition-0-name-2, 2020-03-08T15:12:13.323]",
                        "+I[1, partition-1-name-0, 2020-03-09T13:13:11.123]",
                        "+I[1, partition-1-name-1, 2020-03-09T15:13:11.133]",
                        "+I[1, partition-1-name-2, 2020-03-09T16:13:11.143]",
                        "+I[2, partition-2-name-0, 2020-03-10T13:12:14.123]",
                        "+I[2, partition-2-name-1, 2020-03-10T14:12:14.123]",
                        "+I[2, partition-2-name-2, 2020-03-10T14:13:14.123]",
                        "+I[2, partition-2-name-3, 2020-03-10T14:14:14.123]",
                        "+I[2, partition-2-name-4, 2020-03-10T14:15:14.123]",
                        "+I[2, partition-2-name-5, 2020-03-10T14:16:14.123]",
                        "+I[3, partition-3-name-0, 2020-03-11T17:12:11.123]",
                        "+I[3, partition-3-name-1, 2020-03-11T18:12:11.123]");
        PscTableTestUtils.waitingExpectedResults("MySink", expected, Duration.ofSeconds(5));

        // ------------- cleanup -------------------

        tableResult.getJobClient().ifPresent(JobClient::cancel);
        deleteTestTopic(topic);
    }

    @Test
    public void testPerPartitionWatermarkWithIdleSource() throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "idle_partition_watermark_topic_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 4, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = getStandardProps().getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String bootstraps = getBootstrapServers();
        tEnv.getConfig().set(TABLE_EXEC_SOURCE_IDLE_TIMEOUT, Duration.ofMillis(100));

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `partition_id` INT,\n"
                                + "  `value` INT,\n"
                                + "  `timestamp` TIMESTAMP(3),\n"
                                + "  WATERMARK FOR `timestamp` AS `timestamp`\n"
                                + ") WITH (\n"
                                + "  'connector' = 'psc',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'sink.partitioner' = '%s',\n"
                                + "  'format' = '%s'\n"
                                + ")",
                        topicUri, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX, bootstraps, groupId, TestPartitioner.class.getName(), format);

        tEnv.executeSql(createTable);

        // Only two partitions have elements and others are idle.
        // When idle timer triggers, the WatermarkOutputMultiplexer will use the minimum watermark
        // among active partitions as the output watermark.
        // Therefore, we need to make sure the watermark in the each partition is large enough to
        // trigger the window.
        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " (0, 0, TIMESTAMP '2020-03-08 13:12:11.123'),\n"
                        + " (0, 1, TIMESTAMP '2020-03-08 13:15:12.223'),\n"
                        + " (0, 2, TIMESTAMP '2020-03-08 16:12:13.323'),\n"
                        + " (1, 3, TIMESTAMP '2020-03-08 13:13:11.123'),\n"
                        + " (1, 4, TIMESTAMP '2020-03-08 13:19:11.133'),\n"
                        + " (1, 5, TIMESTAMP '2020-03-08 16:13:11.143')\n";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        env.setParallelism(1);
        String createSink =
                "CREATE TABLE MySink(\n"
                        + "  `id` INT,\n"
                        + "  `cnt` BIGINT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values'\n"
                        + ")";
        tEnv.executeSql(createSink);
        TableResult tableResult =
                tEnv.executeSql(
                        "INSERT INTO MySink\n"
                                + "SELECT `partition_id` as `id`, COUNT(`value`) as `cnt`\n"
                                + "FROM kafka\n"
                                + "GROUP BY `partition_id`, TUMBLE(`timestamp`, INTERVAL '1' HOUR) ");

        final List<String> expected = Arrays.asList("+I[0, 2]", "+I[1, 2]");
        PscTableTestUtils.waitingExpectedResults("MySink", expected, Duration.ofSeconds(5));

        // ------------- cleanup -------------------

        tableResult.getJobClient().ifPresent(JobClient::cancel);
        deleteTestTopic(topic);
    }

    @Test
    public void testLatestOffsetStrategyResume() throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "latest_offset_resume_topic_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 6, 1);
        env.setParallelism(1);

        // ---------- Produce data into Kafka's partition 0-6 -------------------

        String groupId = getStandardProps().getProperty("psc.consumer.group.id");
        String bootstraps = getBootstrapServers();

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `partition_id` INT,\n"
                                + "  `value` INT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'psc',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'latest-offset',\n"
                                + "  'sink.partitioner' = '%s',\n"
                                + "  'format' = '%s'\n"
                                + ")",
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        groupId,
                        TestPartitioner.class.getName(),
                        format
                );

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka VALUES (0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0)";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        String createSink =
                "CREATE TABLE MySink(\n"
                        + "  `id` INT,\n"
                        + "  `value` INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values'\n"
                        + ")";
        tEnv.executeSql(createSink);

        String executeInsert = "INSERT INTO MySink SELECT `partition_id`, `value` FROM kafka";
        TableResult tableResult = tEnv.executeSql(executeInsert);

        // ---------- Produce data into Kafka's partition 0-2 -------------------

        String moreValues = "INSERT INTO kafka VALUES (0, 1), (1, 1), (2, 1)";
        tEnv.executeSql(moreValues).await();

        final List<String> expected = Arrays.asList("+I[0, 1]", "+I[1, 1]", "+I[2, 1]");
        PscTableTestUtils.waitingExpectedResults("MySink", expected, Duration.ofSeconds(5));

        // ---------- Stop the consume job with savepoint  -------------------

        String savepointBasePath = getTempDirPath(topic + "-savepoint");
        assert tableResult.getJobClient().isPresent();
        JobClient client = tableResult.getJobClient().get();
        String savepointPath =
                client.stopWithSavepoint(false, savepointBasePath, SavepointFormatType.DEFAULT)
                        .get();

        // ---------- Produce data into Kafka's partition 0-5 -------------------

        String produceValuesBeforeResume =
                "INSERT INTO kafka VALUES (0, 2), (1, 2), (2, 2), (3, 1), (4, 1), (5, 1)";
        tEnv.executeSql(produceValuesBeforeResume).await();

        // ---------- Resume the consume job from savepoint  -------------------

        Configuration configuration = new Configuration();
        configuration.set(SavepointConfigOptions.SAVEPOINT_PATH, savepointPath);
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        tEnv.executeSql(createTable);
        tEnv.executeSql(createSink);
        tableResult = tEnv.executeSql(executeInsert);

        final List<String> afterResumeExpected =
                Arrays.asList(
                        "+I[0, 1]",
                        "+I[1, 1]",
                        "+I[2, 1]",
                        "+I[0, 2]",
                        "+I[1, 2]",
                        "+I[2, 2]",
                        "+I[3, 1]",
                        "+I[4, 1]",
                        "+I[5, 1]");
        PscTableTestUtils.waitingExpectedResults(
                "MySink", afterResumeExpected, Duration.ofSeconds(5));

        // ---------- Produce data into Kafka's partition 0-5 -------------------

        String produceValuesAfterResume =
                "INSERT INTO kafka VALUES (0, 3), (1, 3), (2, 3), (3, 2), (4, 2), (5, 2)";
        this.tEnv.executeSql(produceValuesAfterResume).await();

        final List<String> afterProduceExpected =
                Arrays.asList(
                        "+I[0, 1]",
                        "+I[1, 1]",
                        "+I[2, 1]",
                        "+I[0, 2]",
                        "+I[1, 2]",
                        "+I[2, 2]",
                        "+I[3, 1]",
                        "+I[4, 1]",
                        "+I[5, 1]",
                        "+I[0, 3]",
                        "+I[1, 3]",
                        "+I[2, 3]",
                        "+I[3, 2]",
                        "+I[4, 2]",
                        "+I[5, 2]");
        PscTableTestUtils.waitingExpectedResults(
                "MySink", afterProduceExpected, Duration.ofSeconds(5));

        // ------------- cleanup -------------------

        tableResult.getJobClient().ifPresent(JobClient::cancel);
        deleteTestTopic(topic);
    }

    @Test
    public void testStartFromGroupOffsetsLatest() throws Exception {
        testStartFromGroupOffsets("latest");
    }

    @Test
    public void testStartFromGroupOffsetsEarliest() throws Exception {
        testStartFromGroupOffsets("earliest");
    }

    @Test
    public void testStartFromGroupOffsetsNone() {
        Assertions.assertThatThrownBy(() -> testStartFromGroupOffsetsWithNoneResetStrategy())
                .satisfies(FlinkAssertions.anyCauseMatches(NoOffsetForPartitionException.class));
    }

    @Test
    public void testAutoGenUuidSourceTableWithClientIdAndGroupId() throws Exception {
        final String topic = "tstopic_autogen_source_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        String bootstraps = getBootstrapServers();

        final String createSourceTable =
                String.format(
                        "CREATE TABLE autogen_source (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  ts TIMESTAMP(3)\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.group.id' = 'AUTO_GEN_UUID',\n"
                                + "  'properties.client.id.prefix' = 'integration-test-source',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        formatOptions());

        tEnv.executeSql(createSourceTable);

        // Create a regular sink table for validation
        final String createSinkTable =
                String.format(
                        "CREATE TABLE validation_sink (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  ts TIMESTAMP(3)\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.producer.client.id' = 'validation-sink-client',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        formatOptions());

        tEnv.executeSql(createSinkTable);

        // Insert test data
        String insertData =
                "INSERT INTO validation_sink\n"
                        + "VALUES\n"
                        + " (1, 'Alice', TIMESTAMP '2023-01-01 10:00:00'),\n"
                        + " (2, 'Bob', TIMESTAMP '2023-01-01 11:00:00'),\n"
                        + " (3, 'Charlie', TIMESTAMP '2023-01-01 12:00:00')";

        tEnv.executeSql(insertData).await();

        // Read from source with AUTO_GEN_UUID properties
        String query = "SELECT id, name FROM autogen_source";
        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(3);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("AutoGenUuid_Source_Test");
        } catch (Throwable e) {
            if (!isCausedByJobFinished(e)) {
                throw e;
            }
        }

        // Verify data was successfully read (proves AUTO_GEN_UUID worked)
        // Order is not guaranteed in streaming, so we check the count and presence of data
        assertThat(TestingSinkFunction.rows).hasSize(3);

        deleteTestTopic(topic);
    }

    @Test
    public void testAutoGenUuidSinkTableWithProducerId() throws Exception {
        final String topic = "tstopic_autogen_sink_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        String bootstraps = getBootstrapServers();

        // Create sink table with AUTO_GEN_UUID for producer client ID
        final String createSinkTable =
                String.format(
                        "CREATE TABLE autogen_sink (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  amount DOUBLE\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.producer.client.id' = 'AUTO_GEN_UUID',\n"
                                + "  'properties.client.id.prefix' = 'integration-test-sink',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        formatOptions());

        tEnv.executeSql(createSinkTable);

        // Create a regular source table for validation
        String groupId = getStandardProps().getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        final String createSourceTable =
                String.format(
                        "CREATE TABLE validation_source (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  amount DOUBLE\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.client.id.prefix' = 'validation-source-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        groupId,
                        formatOptions());

        tEnv.executeSql(createSourceTable);

        // Insert data using sink with AUTO_GEN_UUID producer client ID
        String insertData =
                "INSERT INTO autogen_sink\n"
                        + "VALUES\n"
                        + " (10, 'Product A', 99.99),\n"
                        + " (20, 'Product B', 149.50),\n"
                        + " (30, 'Product C', 299.00)";

        tEnv.executeSql(insertData).await();

        // Read back the data to verify sink worked (proves AUTO_GEN_UUID worked)
        String query = "SELECT id, name, CAST(amount AS DECIMAL(10, 2)) FROM validation_source";
        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(3);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("AutoGenUuid_Sink_Test");
        } catch (Throwable e) {
            if (!isCausedByJobFinished(e)) {
                throw e;
            }
        }

        // Verify data was successfully written and read back (proves AUTO_GEN_UUID worked)
        // Order is not guaranteed in streaming, so we check the count and presence of data
        assertThat(TestingSinkFunction.rows).hasSize(3);

        deleteTestTopic(topic);
    }

    @Test
    public void testMinimumConsumerConfigurationSourceTable() throws Exception {
        final String topic = "tstopic_min_consumer_source_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        String bootstraps = getBootstrapServers();

        // Create source table with minimum required consumer configuration
        final String createSourceTable =
                String.format(
                        "CREATE TABLE min_consumer_source (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  ts TIMESTAMP(3)\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.client.id.prefix' = 'min-consumer-client-id',\n"
                                + "  'properties.psc.consumer.group.id' = 'min-consumer-group-id',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        formatOptions());

        tEnv.executeSql(createSourceTable);

        // Create sink table to write test data
        final String createSinkTable =
                String.format(
                        "CREATE TABLE data_sink (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  ts TIMESTAMP(3)\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.producer.client.id' = 'data-sink-producer-client',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        formatOptions());

        tEnv.executeSql(createSinkTable);

        // Insert test data
        String insertData =
                "INSERT INTO data_sink\n"
                        + "VALUES\n"
                        + " (1, 'MinConfig1', TIMESTAMP '2023-01-01 10:00:00'),\n"
                        + " (2, 'MinConfig2', TIMESTAMP '2023-01-01 11:00:00'),\n"
                        + " (3, 'MinConfig3', TIMESTAMP '2023-01-01 12:00:00')";

        tEnv.executeSql(insertData).await();

        // Read from source with minimum consumer configuration
        String query = "SELECT id, name FROM min_consumer_source";
        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(3);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("MinConsumerConfig_Source_Test");
        } catch (Throwable e) {
            if (!isCausedByJobFinished(e)) {
                throw e;
            }
        }

        // Verify data was successfully read with minimum consumer configuration
        assertThat(TestingSinkFunction.rows).hasSize(3);

        deleteTestTopic(topic);
    }

    @Test
    public void testMinimumProducerConfigurationSinkTable() throws Exception {
        final String topic = "tstopic_min_producer_sink_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        String bootstraps = getBootstrapServers();

        // Create sink table with minimum required producer configuration
        final String createSinkTable =
                String.format(
                        "CREATE TABLE min_producer_sink (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  `value` DOUBLE\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.producer.client.id' = 'min-producer-client-id',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        formatOptions());

        tEnv.executeSql(createSinkTable);

        // Create source table to read back data
        String groupId = getStandardProps().getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        final String createSourceTable =
                String.format(
                        "CREATE TABLE validation_source (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  `value` DOUBLE\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.client.id.prefix' = 'validation-source-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        groupId,
                        formatOptions());

        tEnv.executeSql(createSourceTable);

        // Insert data using sink with minimum producer configuration
        String insertData =
                "INSERT INTO min_producer_sink\n"
                        + "VALUES\n"
                        + " (100, 'MinProd1', 10.5),\n"
                        + " (200, 'MinProd2', 20.75),\n"
                        + " (300, 'MinProd3', 30.25)";

        tEnv.executeSql(insertData).await();

        // Read back the data to verify sink worked with minimum producer configuration
        String query = "SELECT id, name, CAST(`value` AS DECIMAL(10, 2)) FROM validation_source";
        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(3);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("MinProducerConfig_Sink_Test");
        } catch (Throwable e) {
            if (!isCausedByJobFinished(e)) {
                throw e;
            }
        }

        // Verify data was successfully written with minimum producer configuration
        assertThat(TestingSinkFunction.rows).hasSize(3);

        deleteTestTopic(topic);
    }

    @Test
    public void testMinimumConfigurationWithAutoGenUuidSourceTable() throws Exception {
        final String topic = "tstopic_min_autogen_source_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        String bootstraps = getBootstrapServers();

        // Create source table with minimum required configuration using AUTO_GEN_UUID
        final String createSourceTable =
                String.format(
                        "CREATE TABLE min_autogen_source (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  ts TIMESTAMP(3)\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.group.id' = 'AUTO_GEN_UUID',\n"
                                + "  'properties.client.id.prefix' = 'min-config',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        formatOptions());

        tEnv.executeSql(createSourceTable);

        // Create sink table to write test data
        final String createSinkTable =
                String.format(
                        "CREATE TABLE data_sink (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  ts TIMESTAMP(3)\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.producer.client.id' = 'data-sink-producer-client',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        formatOptions());

        tEnv.executeSql(createSinkTable);

        // Insert test data
        String insertData =
                "INSERT INTO data_sink\n"
                        + "VALUES\n"
                        + " (1, 'MinAutoGen1', TIMESTAMP '2023-01-01 10:00:00'),\n"
                        + " (2, 'MinAutoGen2', TIMESTAMP '2023-01-01 11:00:00'),\n"
                        + " (3, 'MinAutoGen3', TIMESTAMP '2023-01-01 12:00:00')";

        tEnv.executeSql(insertData).await();

        // Read from source with minimum configuration using AUTO_GEN_UUID
        String query = "SELECT id, name FROM min_autogen_source";
        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(3);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("MinAutoGenConfig_Source_Test");
        } catch (Throwable e) {
            if (!isCausedByJobFinished(e)) {
                throw e;
            }
        }

        // Verify data was successfully read with minimum AUTO_GEN_UUID configuration
        assertThat(TestingSinkFunction.rows).hasSize(3);

        deleteTestTopic(topic);
    }

    @Test
    public void testMinimumConfigurationWithAutoGenUuidSinkTable() throws Exception {
        final String topic = "tstopic_min_autogen_sink_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        String bootstraps = getBootstrapServers();

        // Create sink table with minimum required configuration using AUTO_GEN_UUID
        final String createSinkTable =
                String.format(
                        "CREATE TABLE min_autogen_sink (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  `value` DOUBLE\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.producer.client.id' = 'AUTO_GEN_UUID',\n"
                                + "  'properties.client.id.prefix' = 'min-sink-config',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        formatOptions());

        tEnv.executeSql(createSinkTable);

        // Create source table to read back data
        String groupId = getStandardProps().getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        final String createSourceTable =
                String.format(
                        "CREATE TABLE validation_source (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  `value` DOUBLE\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.client.id.prefix' = 'validation-source-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        groupId,
                        formatOptions());

        tEnv.executeSql(createSourceTable);

        // Insert data using sink with minimum AUTO_GEN_UUID configuration
        String insertData =
                "INSERT INTO min_autogen_sink\n"
                        + "VALUES\n"
                        + " (1000, 'MinAutoSink1', 100.5),\n"
                        + " (2000, 'MinAutoSink2', 200.75),\n"
                        + " (3000, 'MinAutoSink3', 300.25)";

        tEnv.executeSql(insertData).await();

        // Read back the data to verify sink worked with minimum AUTO_GEN_UUID configuration
        String query = "SELECT id, name, CAST(`value` AS DECIMAL(10, 2)) FROM validation_source";
        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(3);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("MinAutoGenConfig_Sink_Test");
        } catch (Throwable e) {
            if (!isCausedByJobFinished(e)) {
                throw e;
            }
        }

        // Verify data was successfully written with minimum AUTO_GEN_UUID configuration
        assertThat(TestingSinkFunction.rows).hasSize(3);

        deleteTestTopic(topic);
    }

    @Test
    public void testCreateTableLikeLegacyBaseWithAutoGenUuidOverride() throws Exception {
        final String topic = "tstopic_like_legacy_autogen_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        String bootstraps = getBootstrapServers();

        // Create legacy base source table with traditional configuration (specific group id)
        String baseGroupId = getStandardProps().getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        final String createBaseTable =
                String.format(
                        "CREATE TABLE legacy_base_source (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  `value` DOUBLE\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.psc.consumer.client.id' = 'legacy-consumer-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        baseGroupId,
                        formatOptions());

        tEnv.executeSql(createBaseTable);

        // Create derived table using CREATE TABLE ... LIKE with AUTO_GEN_UUID override
        final String createDerivedTable =
                "CREATE TABLE derived_autogen_source\n"
                        + "WITH (\n"
                        + "  'properties.client.id.prefix' = 'derived-autogen-client',\n"
                        + "  'properties.psc.consumer.group.id' = 'AUTO_GEN_UUID'\n"
                        + ") LIKE legacy_base_source";

        tEnv.executeSql(createDerivedTable);

        // Create sink to produce test data
        final String createSink =
                String.format(
                        "CREATE TABLE test_sink (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  `value` DOUBLE\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.client.id.prefix' = 'test-sink-client',\n"
                                + "  'properties.psc.producer.client.id' = 'AUTO_GEN_UUID',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        formatOptions());

        tEnv.executeSql(createSink);

        // Insert test data
        String insertData =
                "INSERT INTO test_sink\n"
                        + "VALUES\n"
                        + " (1100, 'LikeLegacy1', 110.5),\n"
                        + " (1200, 'LikeLegacy2', 120.75)";

        tEnv.executeSql(insertData).await();

        // Verify derived table can read data with AUTO_GEN_UUID configuration
        String query = "SELECT id, name, CAST(`value` AS DECIMAL(10, 2)) FROM derived_autogen_source";
        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(2);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("CreateTableLike_Legacy_AutoGen_Test");
        } catch (Throwable e) {
            if (!isCausedByJobFinished(e)) {
                throw e;
            }
        }

        // Verify data was successfully read using derived table with AUTO_GEN_UUID
        assertThat(TestingSinkFunction.rows).hasSize(2);
        assertThat(TestingSinkFunction.rows).contains("+I(1100,LikeLegacy1,110.50)");
        assertThat(TestingSinkFunction.rows).contains("+I(1200,LikeLegacy2,120.75)");

        deleteTestTopic(topic);
    }

    @Test
    public void testCreateTableLikeAutoGenBaseWithSpecificGroupIdOverride() throws Exception {
        final String topic = "tstopic_like_autogen_specific_" + format + "_" + UUID.randomUUID();
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        createTestTopic(topic, 1, 1);

        String bootstraps = getBootstrapServers();

        // Create base source table with AUTO_GEN_UUID configuration
        final String createBaseTable =
                String.format(
                        "CREATE TABLE autogen_base_source (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  `value` DOUBLE\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.client.id.prefix' = 'autogen-base-client',\n"
                                + "  'properties.psc.consumer.group.id' = 'AUTO_GEN_UUID',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        formatOptions());

        tEnv.executeSql(createBaseTable);

        // Create derived table using CREATE TABLE ... LIKE with specific user-defined group id
        String specificGroupId = "derived-specific-group-" + UUID.randomUUID().toString().substring(0, 8);
        final String createDerivedTable =
                String.format(
                        "CREATE TABLE derived_specific_source\n"
                                + "WITH (\n"
                                + "  'properties.client.id.prefix' = 'derived-specific-client',\n"
                                + "  'properties.psc.consumer.group.id' = '%s'\n"
                                + ") LIKE autogen_base_source",
                        specificGroupId);

        tEnv.executeSql(createDerivedTable);

        // Create sink to produce test data
        final String createSink =
                String.format(
                        "CREATE TABLE test_sink (\n"
                                + "  id INT,\n"
                                + "  name STRING,\n"
                                + "  `value` DOUBLE\n"
                                + ") WITH (\n"
                                + "  'connector' = '%s',\n"
                                + "  'topic-uri' = '%s',\n"
                                + "  'properties.psc.cluster.uri' = '%s',\n"
                                + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                                + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                                + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                                + "  'properties.client.id.prefix' = 'test-sink-client',\n"
                                + "  'properties.psc.producer.client.id' = 'AUTO_GEN_UUID',\n"
                                + "  %s\n"
                                + ")",
                        PscDynamicTableFactory.IDENTIFIER,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        formatOptions());

        tEnv.executeSql(createSink);

        // Insert test data
        String insertData =
                "INSERT INTO test_sink\n"
                        + "VALUES\n"
                        + " (1300, 'LikeSpecific1', 130.5),\n"
                        + " (1400, 'LikeSpecific2', 140.75)";

        tEnv.executeSql(insertData).await();

        // Verify derived table can read data with specific group id
        String query = "SELECT id, name, CAST(`value` AS DECIMAL(10, 2)) FROM derived_specific_source";
        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(2);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("CreateTableLike_AutoGen_Specific_Test");
        } catch (Throwable e) {
            if (!isCausedByJobFinished(e)) {
                throw e;
            }
        }

        // Verify data was successfully read using derived table with specific group id
        assertThat(TestingSinkFunction.rows).hasSize(2);
        assertThat(TestingSinkFunction.rows).contains("+I(1300,LikeSpecific1,130.50)");
        assertThat(TestingSinkFunction.rows).contains("+I(1400,LikeSpecific2,140.75)");

        deleteTestTopic(topic);
    }

    private List<String> appendNewData(
            String topic, String tableName, String groupId, int targetNum) throws Exception {
        waitUtil(
                () -> {
                    Map<TopicPartition, OffsetAndMetadata> offsets = getConsumerOffset(groupId);
                    long sum =
                            offsets.entrySet().stream()
                                    .filter(e -> e.getKey().topic().contains(topic))
                                    .mapToLong(e -> e.getValue().offset())
                                    .sum();
                    return sum == targetNum;
                },
                Duration.ofMillis(20000),
                "Can not reach the expected offset before adding new data.");
        String appendValues =
                "INSERT INTO "
                        + tableName
                        + "\n"
                        + "VALUES\n"
                        + " (2, 6),\n"
                        + " (2, 7),\n"
                        + " (2, 8)\n";
        tEnv.executeSql(appendValues).await();
        return Arrays.asList("+I[2, 6]", "+I[2, 7]", "+I[2, 8]");
    }

    private TableResult startFromGroupOffset(
            String tableName, String topic, String groupId, String resetStrategy, String sinkName)
            throws ExecutionException, InterruptedException {
        final String topicUri = PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX + topic;
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        createTestTopic(topic, 4, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String bootstraps = getBootstrapServers();
        tEnv.getConfig().set(TABLE_EXEC_SOURCE_IDLE_TIMEOUT, Duration.ofMillis(100));

        final String createTableSql =
                "CREATE TABLE %s (\n"
                        + "  `partition_id` INT,\n"
                        + "  `value` INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'psc',\n"
                        + "  'topic-uri' = '%s',\n"
                        + "  'properties.psc.cluster.uri' = '%s',\n"
                        + "  'properties.psc.discovery.topic.uri.prefixes' = '%s',\n"
                        + "  'properties.psc.discovery.connection.urls' = '%s',\n"
                        + "  'properties.psc.discovery.security.protocols' = 'plaintext',\n"
                        + "  'properties.psc.consumer.client.id' = 'psc-test-client',\n"
                        + "  'properties.psc.producer.client.id' = 'psc-test-client',\n"
                        + "  'properties.psc.consumer.group.id' = '%s',\n"
                        + "  'scan.startup.mode' = 'group-offsets',\n"
                        + "  'properties.psc.consumer.offset.auto.reset' = '%s',\n"
                        + "  'properties.psc.consumer.commit.auto.enabled' = 'true',\n"
                        + "  'properties.psc.consumer.auto.commit.interval.ms' = '1000',\n"
                        + "  'format' = '%s'\n"
                        + ")";
        tEnv.executeSql(
                String.format(
                        createTableSql,
                        tableName,
                        topicUri,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER0_URI_PREFIX,
                        bootstraps,
                        groupId,
                        resetStrategy,
                        format));

        String initialValues =
                "INSERT INTO "
                        + tableName
                        + "\n"
                        + "VALUES\n"
                        + " (0, 0),\n"
                        + " (0, 1),\n"
                        + " (0, 2),\n"
                        + " (1, 3),\n"
                        + " (1, 4),\n"
                        + " (1, 5)\n";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        env.setParallelism(1);
        String createSink =
                "CREATE TABLE "
                        + sinkName
                        + "(\n"
                        + "  `partition_id` INT,\n"
                        + "  `value` INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values'\n"
                        + ")";
        tEnv.executeSql(createSink);

        return tEnv.executeSql("INSERT INTO " + sinkName + " SELECT * FROM " + tableName);
    }

    private void testStartFromGroupOffsets(String resetStrategy) throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String tableName = "Table" + format + resetStrategy;
        final String topic =
                "groupOffset_" + format + resetStrategy + ThreadLocalRandom.current().nextLong();
        String groupId = format + resetStrategy;
        String sinkName = "mySink" + format + resetStrategy;
        List<String> expected =
                Arrays.asList(
                        "+I[0, 0]", "+I[0, 1]", "+I[0, 2]", "+I[1, 3]", "+I[1, 4]", "+I[1, 5]");

        TableResult tableResult = null;
        try {
            tableResult = startFromGroupOffset(tableName, topic, groupId, resetStrategy, sinkName);
            if ("latest".equals(resetStrategy)) {
                expected = appendNewData(topic, tableName, groupId, expected.size());
            }
            PscTableTestUtils.waitingExpectedResults(sinkName, expected, Duration.ofSeconds(15));
        } finally {
            // ------------- cleanup -------------------
            if (tableResult != null) {
                tableResult.getJobClient().ifPresent(JobClient::cancel);
            }
            deleteTestTopic(topic);
        }
    }

    private void testStartFromGroupOffsetsWithNoneResetStrategy()
            throws ExecutionException, InterruptedException {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String resetStrategy = "none";
        final String tableName = resetStrategy + "Table";
        final String topic = "groupOffset_" + format + "_" + UUID.randomUUID();
        String groupId = resetStrategy + (new Random()).nextInt();

        TableResult tableResult = null;
        try {
            tableResult = startFromGroupOffset(tableName, topic, groupId, resetStrategy, "MySink");
            tableResult.await();
        } finally {
            // ------------- cleanup -------------------
            if (tableResult != null) {
                tableResult.getJobClient().ifPresent(JobClient::cancel);
            }
            deleteTestTopic(topic);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    /** Extract the partition id from the row and set it on the record. */
    public static class TestPartitioner extends FlinkPscPartitioner<RowData> {

        private static final long serialVersionUID = 1L;
        private static final int PARTITION_ID_FIELD_IN_SCHEMA = 0;

        @Override
        public int partition(
                RowData record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            return partitions[record.getInt(PARTITION_ID_FIELD_IN_SCHEMA) % partitions.length];
        }
    }

    private String formatOptions() {
        return String.format("'format' = '%s'", format);
    }

    private static final class TestingSinkFunction implements SinkFunction<RowData> {

        private static final long serialVersionUID = 455430015321124493L;
        private static List<String> rows = new ArrayList<>();

        private final int expectedSize;

        private TestingSinkFunction(int expectedSize) {
            this.expectedSize = expectedSize;
            rows.clear();
        }

        @Override
        public void invoke(RowData value, Context context) {
            rows.add(value.toString());
            if (rows.size() >= expectedSize) {
                // job finish
                throw new SuccessException();
            }
        }
    }

    private static boolean isCausedByJobFinished(Throwable e) {
        if (e instanceof SuccessException) {
            return true;
        } else if (e.getCause() != null) {
            return isCausedByJobFinished(e.getCause());
        } else {
            return false;
        }
    }
}
