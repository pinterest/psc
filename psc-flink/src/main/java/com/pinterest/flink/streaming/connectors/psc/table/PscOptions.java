/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.flink.streaming.connectors.psc.table;

import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import com.pinterest.psc.config.PscConfiguration;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import com.pinterest.flink.streaming.connectors.psc.config.StartupMode;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Option utils for PSC table source sink.
 */
public class PscOptions {
    private PscOptions() {
    }

    // --------------------------------------------------------------------------------------------
    // PSC specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> TOPIC_URI = ConfigOptions
            .key("topic")
            .stringType()
            .noDefaultValue()
            .withDescription("Required topic URI from which the table is read");

//	public static final ConfigOption<String> PROPS_BOOTSTRAP_SERVERS = ConfigOptions
//			.key("properties.bootstrap.servers")
//			.stringType()
//			.noDefaultValue()
//			.withDescription("Required Kafka server connection string");

    public static final ConfigOption<String> PROPS_GROUP_ID = ConfigOptions
            .key("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID)
            .stringType()
            .noDefaultValue()
            .withDescription("Required consumer group in PSC consumer, no need for PSC producer");

    // --------------------------------------------------------------------------------------------
    // Scan specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> SCAN_STARTUP_MODE = ConfigOptions
            .key("scan.startup.mode")
            .stringType()
            .defaultValue("group-offsets")
            .withDescription("Optional startup mode for PSC consumer, valid enumerations are "
                    + "\"earliest-offset\", \"latest-offset\", \"group-offsets\", \"timestamp\"\n"
                    + "or \"specific-offsets\"");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSETS = ConfigOptions
            .key("scan.startup.specific-offsets")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional offsets used in case of \"specific-offsets\" startup mode");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS = ConfigOptions
            .key("scan.startup.timestamp-millis")
            .longType()
            .noDefaultValue()
            .withDescription("Optional timestamp used in case of \"timestamp\" startup mode");

    // --------------------------------------------------------------------------------------------
    // Sink specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> SINK_PARTITIONER = ConfigOptions
            .key("sink.partitioner")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional output partitioning from Flink's partitions\n"
                    + "into PSC's topic URI partitions valid enumerations are\n"
                    + "\"fixed\": (each Flink partition ends up in at most one PSC partition),\n"
                    + "\"round-robin\": (a Flink partition is distributed to PSC partitions round-robin)\n"
                    + "\"custom class name\": (use a custom FlinkPscPartitioner subclass)");

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------

    // Start up offset.
    public static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
    public static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    public static final String SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS = "group-offsets";
    public static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS = "specific-offsets";
    public static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static final Set<String> SCAN_STARTUP_MODE_ENUMS = new HashSet<>(Arrays.asList(
            SCAN_STARTUP_MODE_VALUE_EARLIEST,
            SCAN_STARTUP_MODE_VALUE_LATEST,
            SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS,
            SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS,
            SCAN_STARTUP_MODE_VALUE_TIMESTAMP));

    // Sink partitioner.
    public static final String SINK_PARTITIONER_VALUE_FIXED = "fixed";
    public static final String SINK_PARTITIONER_VALUE_ROUND_ROBIN = "round-robin";

    private static final Set<String> SINK_PARTITIONER_ENUMS = new HashSet<>(Arrays.asList(
            SINK_PARTITIONER_VALUE_FIXED,
            SINK_PARTITIONER_VALUE_ROUND_ROBIN));

    // Prefix for PSC specific properties.
    public static final String PROPERTIES_PREFIX = "properties.";

    // Other keywords.
    private static final String PARTITION = "partition";
    private static final String OFFSET = "offset";

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    public static void validateTableOptions(ReadableConfig tableOptions) {
        validateScanStartupMode(tableOptions);
        validateSinkPartitioner(tableOptions);
    }

    private static void validateScanStartupMode(ReadableConfig tableOptions) {
        tableOptions.getOptional(SCAN_STARTUP_MODE)
                .map(String::toLowerCase)
                .ifPresent(mode -> {
                    if (!SCAN_STARTUP_MODE_ENUMS.contains(mode)) {
                        throw new ValidationException(
                                String.format("Invalid value for option '%s'. Supported values are %s, but was: %s",
                                        SCAN_STARTUP_MODE.key(),
                                        "[earliest-offset, latest-offset, group-offsets, specific-offsets, timestamp]",
                                        mode));
                    }

                    if (mode.equals(SCAN_STARTUP_MODE_VALUE_TIMESTAMP)) {
                        if (!tableOptions.getOptional(SCAN_STARTUP_TIMESTAMP_MILLIS).isPresent()) {
                            throw new ValidationException(String.format("'%s' is required in '%s' startup mode"
                                            + " but missing.",
                                    SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                                    SCAN_STARTUP_MODE_VALUE_TIMESTAMP));
                        }
                    }
                    if (mode.equals(SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS)) {
                        if (!tableOptions.getOptional(SCAN_STARTUP_SPECIFIC_OFFSETS).isPresent()) {
                            throw new ValidationException(String.format("'%s' is required in '%s' startup mode"
                                            + " but missing.",
                                    SCAN_STARTUP_SPECIFIC_OFFSETS.key(),
                                    SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS));
                        }
                        String specificOffsets = tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
                        parseSpecificOffsets(specificOffsets, SCAN_STARTUP_SPECIFIC_OFFSETS.key());
                    }
                });
    }

    private static void validateSinkPartitioner(ReadableConfig tableOptions) {
        tableOptions.getOptional(SINK_PARTITIONER)
                .ifPresent(partitioner -> {
                    if (!SINK_PARTITIONER_ENUMS.contains(partitioner.toLowerCase())) {
                        if (partitioner.isEmpty()) {
                            throw new ValidationException(
                                    String.format("Option '%s' should be a non-empty string.",
                                            SINK_PARTITIONER.key()));
                        }
                    }
                });
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static StartupOptions getStartupOptions(
            ReadableConfig tableOptions,
            String topicUri) {
        final Map<PscTopicUriPartition, Long> specificOffsets = new HashMap<>();
        final StartupMode startupMode = tableOptions.getOptional(SCAN_STARTUP_MODE)
                .map(modeString -> {
                    switch (modeString) {
                        case SCAN_STARTUP_MODE_VALUE_EARLIEST:
                            return StartupMode.EARLIEST;

                        case SCAN_STARTUP_MODE_VALUE_LATEST:
                            return StartupMode.LATEST;

                        case SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS:
                            return StartupMode.GROUP_OFFSETS;

                        case SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS:
                            buildSpecificOffsets(tableOptions, topicUri, specificOffsets);
                            return StartupMode.SPECIFIC_OFFSETS;

                        case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                            return StartupMode.TIMESTAMP;

                        default:
                            throw new TableException("Unsupported startup mode. Validator should have checked that.");
                    }
                }).orElse(StartupMode.GROUP_OFFSETS);
        final StartupOptions options = new StartupOptions();
        options.startupMode = startupMode;
        options.specificOffsets = specificOffsets;
        if (startupMode == StartupMode.TIMESTAMP) {
            options.startupTimestampMillis = tableOptions.get(SCAN_STARTUP_TIMESTAMP_MILLIS);
        }
        return options;
    }

    private static void buildSpecificOffsets(
            ReadableConfig tableOptions,
            String topicUri,
            Map<PscTopicUriPartition, Long> specificOffsets) {
        String specificOffsetsStrOpt = tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
        final Map<Integer, Long> offsetMap = parseSpecificOffsets(
                specificOffsetsStrOpt,
                SCAN_STARTUP_SPECIFIC_OFFSETS.key());
        offsetMap.forEach((partition, offset) -> {
            final PscTopicUriPartition topicPartition = new PscTopicUriPartition(topicUri, partition);
            specificOffsets.put(topicPartition, offset);
        });
    }

    public static Properties getPscProperties(Map<String, String> tableOptions) {
        final Properties pscProperties = new Properties();

        if (hasPscClientProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(key -> {
                        final String value = tableOptions.get(key);
                        final String subKey = key.substring((PROPERTIES_PREFIX).length());
                        pscProperties.put(subKey, value);
                    });
        }
        return pscProperties;
    }

    /**
     * The partitioner can be either "fixed", "round-robin" or a customized partitioner full class name.
     */
    public static Optional<FlinkPscPartitioner<RowData>> getFlinkPscPartitioner(
            ReadableConfig tableOptions,
            ClassLoader classLoader) {
        return tableOptions.getOptional(SINK_PARTITIONER)
                .flatMap((String partitioner) -> {
                    switch (partitioner) {
                        case SINK_PARTITIONER_VALUE_FIXED:
                            return Optional.of(new FlinkFixedPartitioner<>());
                        case SINK_PARTITIONER_VALUE_ROUND_ROBIN:
                            return Optional.empty();
                        // Default fallback to full class name of the partitioner.
                        default:
                            return Optional.of(initializePartitioner(partitioner, classLoader));
                    }
                });
    }

    /**
     * Parses SpecificOffsets String to Map.
     *
     * <p>SpecificOffsets String format was given as following:
     *
     * <pre>
     *     scan.startup.specific-offsets = partition:0,offset:42;partition:1,offset:300
     * </pre>
     *
     * @return SpecificOffsets with Map format, key is partition, and value is offset
     */
    public static Map<Integer, Long> parseSpecificOffsets(
            String specificOffsetsStr,
            String optionKey) {
        final Map<Integer, Long> offsetMap = new HashMap<>();
        final String[] pairs = specificOffsetsStr.split(";");
        final String validationExceptionMessage = String.format(
                "Invalid properties '%s' should follow the format "
                        + "'partition:0,offset:42;partition:1,offset:300', but is '%s'.",
                optionKey,
                specificOffsetsStr);

        if (pairs.length == 0) {
            throw new ValidationException(validationExceptionMessage);
        }

        for (String pair : pairs) {
            if (null == pair || pair.length() == 0 || !pair.contains(",")) {
                throw new ValidationException(validationExceptionMessage);
            }

            final String[] kv = pair.split(",");
            if (kv.length != 2 ||
                    !kv[0].startsWith(PARTITION + ':') ||
                    !kv[1].startsWith(OFFSET + ':')) {
                throw new ValidationException(validationExceptionMessage);
            }

            String partitionValue = kv[0].substring(kv[0].indexOf(":") + 1);
            String offsetValue = kv[1].substring(kv[1].indexOf(":") + 1);
            try {
                final Integer partition = Integer.valueOf(partitionValue);
                final Long offset = Long.valueOf(offsetValue);
                offsetMap.put(partition, offset);
            } catch (NumberFormatException e) {
                throw new ValidationException(validationExceptionMessage, e);
            }
        }
        return offsetMap;
    }

    /**
     * Decides if the table options contains PSC client properties that start with prefix 'properties'.
     */
    private static boolean hasPscClientProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }

    /**
     * Returns a class value with the given class name.
     */
    private static <T> FlinkPscPartitioner<T> initializePartitioner(String name, ClassLoader classLoader) {
        try {
            Class<?> clazz = Class.forName(name, true, classLoader);
            if (!FlinkPscPartitioner.class.isAssignableFrom(clazz)) {
                throw new ValidationException(
                        String.format("Sink partitioner class '%s' should extend from the required class %s",
                                name,
                                FlinkPscPartitioner.class.getName()));
            }
            @SuppressWarnings("unchecked") final FlinkPscPartitioner<T> pscPartitioner = InstantiationUtil.instantiate(name, FlinkPscPartitioner.class, classLoader);

            return pscPartitioner;
        } catch (ClassNotFoundException | FlinkException e) {
            throw new ValidationException(
                    String.format("Could not find and instantiate partitioner class '%s'", name), e);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Inner classes
    // --------------------------------------------------------------------------------------------

    /**
     * PSC startup options.
     **/
    public static class StartupOptions {
        public StartupMode startupMode;
        public Map<PscTopicUriPartition, Long> specificOffsets;
        public long startupTimestampMillis;
    }
}
