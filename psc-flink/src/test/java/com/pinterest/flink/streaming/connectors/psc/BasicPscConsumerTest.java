package com.pinterest.flink.streaming.connectors.psc;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.util.Properties;

public class BasicPscConsumerTest extends PscConsumerTestBaseWithKafkaAsPubSub {

    public static void printProperties(Properties prop) {
        for (Object key : prop.keySet()) {
            System.out.println(key + ": " + prop.getProperty(key.toString()));
        }
    }

    //  @Test
    public void testSimpleConsumptionCount() throws Exception {
        final int parallelism = 1;
        final int recordsInEachPartition = 50;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(parallelism);
        env.enableCheckpointing(200);

        int numPartitions = parallelism;
        String topicName = "test_simple_count";
        createTestTopic(PscTestEnvironmentWithKafkaAsPubSub.PSC_TEST_CLUSTER1_URI_PREFIX + topicName, numPartitions, 1);
        pscTestEnvWithKafka.produceToKafka(topicName, recordsInEachPartition, numPartitions, "hello");

        Properties readProps = new Properties();
        readProps.putAll(standardPscConsumerConfiguration);
        printProperties(readProps);

        DataStream<String> stream = env
                .addSource(pscTestEnvWithKafka.getPscConsumer(topicName, new SimpleStringSchema(), readProps));
        stream.addSink(new DiscardingSink<String>());
        env.execute("test_simple_count");

        deleteTestTopic(topicName);
    }

}
