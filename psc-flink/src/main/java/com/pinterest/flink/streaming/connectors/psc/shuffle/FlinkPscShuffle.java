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

package com.pinterest.flink.streaming.connectors.psc.shuffle;

import com.pinterest.psc.common.TopicUri;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import com.pinterest.flink.streaming.connectors.psc.FlinkPscProducer;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.PropertiesUtil;

import java.util.Properties;

/**
 * {@link FlinkPscShuffle} uses Kafka as a message bus to shuffle and persist data at the same time.
 *
 * <p>Persisting shuffle data is useful when
 * - you would like to reuse the shuffle data and/or,
 * - you would like to avoid a full restart of a pipeline during failure recovery
 *
 * <p>Persisting shuffle is achieved by wrapping a {@link FlinkPscShuffleProducer} and
 * a {@link FlinkPscShuffleConsumer} together into a {@link FlinkPscShuffle}.
 * Here is an example how to use a {@link FlinkPscShuffle}.
 *
 * <p><pre>{@code
 * 	StreamExecutionEnvironment env = ...                    // create execution environment
 * 	DataStream<X> source = env.addSource(...)                // add data stream source
 * 	DataStream<Y> dataStream = ...                            // some transformation(s) based on source
 *
 * 	KeyedStream<Y, KEY> keyedStream = FlinkKafkaShuffle
 * 		.persistentKeyBy(                                    // keyBy shuffle through kafka
 * 			dataStream,                                        // data stream to be shuffled
 * 			topic,                                            // Kafka topic written to
 * 			producerParallelism,                            // the number of tasks of a Kafka Producer
 * 			numberOfPartitions,                                // the number of partitions of the Kafka topic written to
 * 			kafkaProperties,                                // kafka properties for Kafka Producer and Consumer
 * 			keySelector<Y, KEY>);                            // key selector to retrieve key from `dataStream'
 *
 * 	keyedStream.transform...                                // some other transformation(s)
 *
 * 	KeyedStream<Y, KEY> keyedStreamReuse = FlinkKafkaShuffle
 * 		.readKeyBy(                                            // Read the Kafka shuffle data again for other usages
 * 			topic,                                            // the topic of Kafka where data is persisted
 * 			env,                                            // execution environment, and it can be a new environment
 * 			typeInformation<Y>,                                // type information of the data persisted in Kafka
 * 			kafkaProperties,                                // kafka properties for Kafka Consumer
 * 			keySelector<Y, KEY>);                            // key selector to retrieve key
 *
 * 	keyedStreamReuse.transform...                            // some other transformation(s)
 * }</pre>
 *
 * <p>Usage of {@link FlinkPscShuffle#persistentKeyBy} is similar to {@link DataStream#keyBy(KeySelector)}.
 * The differences are:
 *
 * <p>1). Partitioning is done through {@link FlinkPscShuffleProducer}. {@link FlinkPscShuffleProducer} decides
 * which partition a key goes when writing to Kafka
 *
 * <p>2). Shuffle data can be reused through {@link FlinkPscShuffle#readKeyBy}, as shown in the example above.
 *
 * <p>3). Job execution is decoupled by the persistent Kafka message bus. In the example, the job execution graph is
 * decoupled to three regions: `KafkaShuffleProducer', `KafkaShuffleConsumer' and `KafkaShuffleConsumerReuse'
 * through `PERSISTENT DATA` as shown below. If any region fails the execution, the other two keep progressing.
 *
 * <p><pre>
 *     source -> ... KafkaShuffleProducer -> PERSISTENT DATA -> KafkaShuffleConsumer -> ...
 *                                                |
 *                                                | ----------> KafkaShuffleConsumerReuse -> ...
 * </pre>
 */
@Experimental
public class FlinkPscShuffle {
    static final String PRODUCER_PARALLELISM = "producer parallelism";
    static final String PARTITION_NUMBER = "partition number";

    /**
     * Uses Kafka as a message bus to persist keyBy shuffle.
     *
     * <p>Persisting keyBy shuffle is achieved by wrapping a {@link FlinkPscShuffleProducer} and
     * {@link FlinkPscShuffleConsumer} together.
     *
     * <p>On the producer side, {@link FlinkPscShuffleProducer}
     * is similar to {@link DataStream#keyBy(KeySelector)}. They use the same key group assignment function
     * {@link KeyGroupRangeAssignment#assignKeyToParallelOperator} to decide which partition a key goes.
     * Hence, each producer task can potentially write to each Kafka partition based on where the key goes.
     * Here, `numberOfPartitions` equals to the key group size.
     * In the case of using {@link TimeCharacteristic#EventTime}, each producer task broadcasts its watermark
     * to ALL of the Kafka partitions to make sure watermark information is propagated correctly.
     *
     * <p>On the consumer side, each consumer task should read partitions equal to the key group indices
     * it is assigned. `numberOfPartitions` is the maximum parallelism of the consumer. This version only
     * supports numberOfPartitions = consumerParallelism.
     * In the case of using {@link TimeCharacteristic#EventTime}, a consumer task is responsible to emit
     * watermarks. Watermarks are read from the corresponding Kafka partitions. Notice that a consumer task only starts
     * to emit a watermark after reading at least one watermark from each producer task to make sure watermarks
     * are monotonically increasing. Hence a consumer task needs to know `producerParallelism` as well.
     *
     * @param dataStream          Data stream to be shuffled
     * @param topicUri            PSC topic URI written to
     * @param producerParallelism Parallelism of producer
     * @param numberOfPartitions  Number of partitions
     * @param configuration       Kafka properties
     * @param keySelector         Key selector to retrieve key from `dataStream'
     * @param <T>                 Type of the input data stream
     * @param <K>                 Type of key
     * @see FlinkPscShuffle#writeKeyBy
     * @see FlinkPscShuffle#readKeyBy
     */
    public static <T, K> KeyedStream<T, K> persistentKeyBy(
            DataStream<T> dataStream,
            TopicUri topicUri,
            int producerParallelism,
            int numberOfPartitions,
            Properties configuration,
            KeySelector<T, K> keySelector) {
        // KafkaProducer#propsToMap uses Properties purely as a HashMap without considering the default properties
        // So we have to flatten the default property to first level elements.
        configuration.setProperty(PRODUCER_PARALLELISM, String.valueOf(producerParallelism));
        configuration.setProperty(PARTITION_NUMBER, String.valueOf(numberOfPartitions));

        StreamExecutionEnvironment env = dataStream.getExecutionEnvironment();

        writeKeyBy(dataStream, topicUri.getTopicUriAsString(), configuration, keySelector);
        return readKeyBy(topicUri.getTopicUriAsString(), env, dataStream.getType(), configuration, keySelector);
    }

    /**
     * Uses Kafka as a message bus to persist keyBy shuffle.
     *
     * <p>Persisting keyBy shuffle is achieved by wrapping a {@link FlinkPscShuffleProducer} and
     * {@link FlinkPscShuffleConsumer} together.
     *
     * <p>On the producer side, {@link FlinkPscShuffleProducer}
     * is similar to {@link DataStream#keyBy(KeySelector)}. They use the same key group assignment function
     * {@link KeyGroupRangeAssignment#assignKeyToParallelOperator} to decide which partition a key goes.
     * Hence, each producer task can potentially write to each Kafka partition based on where the key goes.
     * Here, `numberOfPartitions` equals to the key group size.
     * In the case of using {@link TimeCharacteristic#EventTime}, each producer task broadcasts its watermark
     * to ALL of the Kafka partitions to make sure watermark information is propagated correctly.
     *
     * <p>On the consumer side, each consumer task should read partitions equal to the key group indices
     * it is assigned. `numberOfPartitions` is the maximum parallelism of the consumer. This version only
     * supports numberOfPartitions = consumerParallelism.
     * In the case of using {@link TimeCharacteristic#EventTime}, a consumer task is responsible to emit
     * watermarks. Watermarks are read from the corresponding Kafka partitions. Notice that a consumer task only starts
     * to emit a watermark after reading at least one watermark from each producer task to make sure watermarks
     * are monotonically increasing. Hence a consumer task needs to know `producerParallelism` as well.
     *
     * @param dataStream          Data stream to be shuffled
     * @param topicUri            Kafka topic uri written to
     * @param producerParallelism Parallelism of producer
     * @param numberOfPartitions  Number of partitions
     * @param configuration       Kafka properties
     * @param fields              Key positions from the input data stream
     * @param <T>                 Type of the input data stream
     * @see FlinkPscShuffle#writeKeyBy
     * @see FlinkPscShuffle#readKeyBy
     */
    public static <T> KeyedStream<T, Tuple> persistentKeyBy(
            DataStream<T> dataStream,
            TopicUri topicUri,
            int producerParallelism,
            int numberOfPartitions,
            Properties configuration,
            int... fields) {
        return persistentKeyBy(
                dataStream,
                topicUri,
                producerParallelism,
                numberOfPartitions,
                configuration,
                keySelector(dataStream, fields));
    }

    /**
     * The write side of {@link FlinkPscShuffle#persistentKeyBy}.
     *
     * <p>This function contains a {@link FlinkPscShuffleProducer} to shuffle and persist data in Kafka.
     * {@link FlinkPscShuffleProducer} uses the same key group assignment function
     * {@link KeyGroupRangeAssignment#assignKeyToParallelOperator} to decide which partition a key goes.
     * Hence, each producer task can potentially write to each Kafka partition based on the key.
     * Here, the number of partitions equals to the key group size.
     * In the case of using {@link TimeCharacteristic#EventTime}, each producer task broadcasts each watermark
     * to all of the Kafka partitions to make sure watermark information is propagated properly.
     *
     * <p>Attention: make sure kafkaProperties include
     * {@link FlinkPscShuffle#PRODUCER_PARALLELISM} and {@link FlinkPscShuffle#PARTITION_NUMBER} explicitly.
     * {@link FlinkPscShuffle#PRODUCER_PARALLELISM} is the parallelism of the producer.
     * {@link FlinkPscShuffle#PARTITION_NUMBER} is the number of partitions.
     * They are not necessarily the same and allowed to be set independently.
     *
     * @param dataStream               Data stream to be shuffled
     * @param topicUri                 PSC topic URI written to
     * @param pscProducerConfiguration PSC properties for PSC Producer
     * @param keySelector              Key selector to retrieve key from `dataStream'
     * @param <T>                      Type of the input data stream
     * @param <K>                      Type of key
     * @see FlinkPscShuffle#persistentKeyBy
     * @see FlinkPscShuffle#readKeyBy
     */
    public static <T, K> void writeKeyBy(
            DataStream<T> dataStream,
            String topicUri,
            Properties pscProducerConfiguration,
            KeySelector<T, K> keySelector) {

        StreamExecutionEnvironment env = dataStream.getExecutionEnvironment();
        TypeSerializer<T> typeSerializer = dataStream.getType().createSerializer(env.getConfig());

        // write data to Kafka
        FlinkPscShuffleProducer<T, K> pscProducer = new FlinkPscShuffleProducer<>(
                topicUri,
                typeSerializer,
                pscProducerConfiguration,
                env.clean(keySelector),
                FlinkPscProducer.Semantic.EXACTLY_ONCE,
                FlinkPscProducer.DEFAULT_PSC_PRODUCERS_POOL_SIZE);

        // make sure the sink parallelism is set to producerParallelism
        Preconditions.checkArgument(
                pscProducerConfiguration.getProperty(PRODUCER_PARALLELISM) != null,
                "Missing producer parallelism for Kafka Shuffle");
        int producerParallelism = PropertiesUtil.getInt(pscProducerConfiguration, PRODUCER_PARALLELISM, Integer.MIN_VALUE);

        addKafkaShuffle(dataStream, pscProducer, producerParallelism);
    }

    /**
     * The write side of {@link FlinkPscShuffle#persistentKeyBy}.
     *
     * <p>This function contains a {@link FlinkPscShuffleProducer} to shuffle and persist data in Kafka.
     * {@link FlinkPscShuffleProducer} uses the same key group assignment function
     * {@link KeyGroupRangeAssignment#assignKeyToParallelOperator} to decide which partition a key goes.
     *
     * <p>Hence, each producer task can potentially write to each Kafka partition based on the key.
     * Here, the number of partitions equals to the key group size.
     * In the case of using {@link TimeCharacteristic#EventTime}, each producer task broadcasts each watermark
     * to all of the Kafka partitions to make sure watermark information is propagated properly.
     *
     * <p>Attention: make sure kafkaProperties include
     * {@link FlinkPscShuffle#PRODUCER_PARALLELISM} and {@link FlinkPscShuffle#PARTITION_NUMBER} explicitly.
     * {@link FlinkPscShuffle#PRODUCER_PARALLELISM} is the parallelism of the producer.
     * {@link FlinkPscShuffle#PARTITION_NUMBER} is the number of partitions.
     * They are not necessarily the same and allowed to be set independently.
     *
     * @param dataStream    Data stream to be shuffled
     * @param topicUri      Kafka topic written to
     * @param configuration PSC configuration for PSC Producer
     * @param fields        Key positions from the input data stream
     * @param <T>           Type of the input data stream
     * @see FlinkPscShuffle#persistentKeyBy
     * @see FlinkPscShuffle#readKeyBy
     */
    public static <T> void writeKeyBy(
            DataStream<T> dataStream,
            String topicUri,
            Properties configuration,
            int... fields) {
        writeKeyBy(dataStream, topicUri, configuration, keySelector(dataStream, fields));
    }

    /**
     * The read side of {@link FlinkPscShuffle#persistentKeyBy}.
     *
     * <p>Each consumer task should read kafka partitions equal to the key group indices it is assigned.
     * The number of kafka partitions is the maximum parallelism of the consumer.
     * This version only supports numberOfPartitions = consumerParallelism.
     * In the case of using {@link TimeCharacteristic#EventTime}, a consumer task is responsible to emit
     * watermarks. Watermarks are read from the corresponding Kafka partitions. Notice that a consumer task only starts
     * to emit a watermark after receiving at least one watermark from each producer task to make sure watermarks
     * are monotonically increasing. Hence a consumer task needs to know `producerParallelism` as well.
     *
     * <p>Attention: make sure kafkaProperties include
     * {@link FlinkPscShuffle#PRODUCER_PARALLELISM} and {@link FlinkPscShuffle#PARTITION_NUMBER} explicitly.
     * {@link FlinkPscShuffle#PRODUCER_PARALLELISM} is the parallelism of the producer.
     * {@link FlinkPscShuffle#PARTITION_NUMBER} is the number of partitions.
     * They are not necessarily the same and allowed to be set independently.
     *
     * @param topicUri                 The topic of Kafka where data is persisted
     * @param env                      Execution environment. readKeyBy's environment can be different from writeKeyBy's
     * @param typeInformation          Type information of the data persisted in Kafka
     * @param pscConsumerConfiguration kafka properties for Kafka Consumer
     * @param keySelector              key selector to retrieve key
     * @param <T>                      Schema type
     * @param <K>                      Key type
     * @return Keyed data stream
     * @see FlinkPscShuffle#persistentKeyBy
     * @see FlinkPscShuffle#writeKeyBy
     */
    public static <T, K> KeyedStream<T, K> readKeyBy(
            String topicUri,
            StreamExecutionEnvironment env,
            TypeInformation<T> typeInformation,
            Properties pscConsumerConfiguration,
            KeySelector<T, K> keySelector) {

        TypeSerializer<T> typeSerializer = typeInformation.createSerializer(env.getConfig());
        TypeInformationSerializationSchema<T> schema =
                new TypeInformationSerializationSchema<>(typeInformation, typeSerializer);

        SourceFunction<T> pscConsumer =
                new FlinkPscShuffleConsumer<>(topicUri, schema, typeSerializer, pscConsumerConfiguration);

        // TODO: consider situations where numberOfPartitions != consumerParallelism
        Preconditions.checkArgument(
                pscConsumerConfiguration.getProperty(PARTITION_NUMBER) != null,
                "Missing partition number for Kafka Shuffle");
        int numberOfPartitions = PropertiesUtil.getInt(pscConsumerConfiguration, PARTITION_NUMBER, Integer.MIN_VALUE);
        DataStream<T> outputDataStream = env.addSource(pscConsumer).setParallelism(numberOfPartitions);

        return DataStreamUtils.reinterpretAsKeyedStream(outputDataStream, keySelector);
    }

    /**
     * Adds a {@link StreamPscShuffleSink} to {@link DataStream}.
     *
     * <p>{@link StreamPscShuffleSink} is associated a {@link FlinkPscShuffleProducer}.
     *
     * @param inputStream          Input data stream connected to the shuffle
     * @param kafkaShuffleProducer Kafka shuffle sink function that can handle both records and watermark
     * @param producerParallelism  The number of tasks writing to the kafka shuffle
     */
    private static <T, K> void addKafkaShuffle(
            DataStream<T> inputStream,
            FlinkPscShuffleProducer<T, K> kafkaShuffleProducer,
            int producerParallelism) {

        // read the output type of the input Transform to coax out errors about MissingTypeInfo
        inputStream.getTransformation().getOutputType();

        StreamPscShuffleSink<T> shuffleSinkOperator = new StreamPscShuffleSink<>(kafkaShuffleProducer);
        SinkTransformation<T> transformation = new SinkTransformation<>(
                inputStream.getTransformation(),
                "kafka_shuffle",
                shuffleSinkOperator,
                inputStream.getExecutionEnvironment().getParallelism());
        inputStream.getExecutionEnvironment().addOperator(transformation);
        transformation.setParallelism(producerParallelism);
    }

    // A better place to put this function is DataStream; but put it here for now to avoid changing DataStream
    private static <T> KeySelector<T, Tuple> keySelector(DataStream<T> source, int... fields) {
        KeySelector<T, Tuple> keySelector;
        if (source.getType() instanceof BasicArrayTypeInfo || source.getType() instanceof PrimitiveArrayTypeInfo) {
            keySelector = KeySelectorUtil.getSelectorForArray(fields, source.getType());
        } else {
            Keys<T> keys = new Keys.ExpressionKeys<>(fields, source.getType());
            keySelector = KeySelectorUtil.getSelectorForKeys(
                    keys,
                    source.getType(),
                    source.getExecutionEnvironment().getConfig());
        }

        return keySelector;
    }
}
