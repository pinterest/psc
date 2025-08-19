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

import com.pinterest.flink.streaming.connectors.psc.config.BoundedMode;
import com.pinterest.flink.streaming.connectors.psc.config.StartupMode;
import com.pinterest.flink.streaming.connectors.psc.internals.PscTopicUriPartition;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkFixedPartitioner;
import com.pinterest.flink.streaming.connectors.psc.partitioner.FlinkPscPartitioner;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import com.pinterest.psc.config.PscConfiguration;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.DELIVERY_GUARANTEE;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.KEY_FIELDS;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.KEY_FIELDS_PREFIX;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.KEY_FORMAT;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.SCAN_BOUNDED_MODE;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.SCAN_BOUNDED_SPECIFIC_OFFSETS;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.SCAN_BOUNDED_TIMESTAMP_MILLIS;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.SCAN_STARTUP_MODE;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.SINK_PARTITIONER;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.TOPIC_URI;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.TOPIC_PATTERN;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.TRANSACTIONAL_ID_PREFIX;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.VALUE_FIELDS_INCLUDE;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.VALUE_FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/** Utilities for {@link PscConnectorOptions}. */
@Internal
class PscConnectorOptionsUtil {

    private static final ConfigOption<String> SCHEMA_REGISTRY_SUBJECT =
            ConfigOptions.key("schema-registry.subject").stringType().noDefaultValue();

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------

    // Sink partitioner.
    public static final String SINK_PARTITIONER_VALUE_DEFAULT = "default";
    public static final String SINK_PARTITIONER_VALUE_FIXED = "fixed";
    public static final String SINK_PARTITIONER_VALUE_ROUND_ROBIN = "round-robin";

    // Prefix for PSC specific properties.
    public static final String PROPERTIES_PREFIX = "properties.";

    // AUTO_GEN_UUID keyword for UUID generation.
    public static final String AUTO_GEN_UUID_VALUE = "AUTO_GEN_UUID";

    // Other keywords.
    private static final String PARTITION = "partition";
    private static final String OFFSET = "offset";
    protected static final String AVRO_CONFLUENT = "avro-confluent";
    protected static final String DEBEZIUM_AVRO_CONFLUENT = "debezium-avro-confluent";
    private static final List<String> SCHEMA_REGISTRY_FORMATS =
            Arrays.asList(AVRO_CONFLUENT, DEBEZIUM_AVRO_CONFLUENT);

    /**
     * Validates if AUTO_GEN_UUID is allowed for the given property key and that app.prefix is provided.
     * Only allows AUTO_GEN_UUID for psc.consumer.client.id, psc.consumer.group.id, and psc.producer.client.id keys.
     * Throws ValidationException if AUTO_GEN_UUID is not allowed for the key or if app.prefix is missing/empty.
     *
     * @param subKey the property key (without the "properties." prefix)
     * @param tableOptions the table options map to check for app.prefix
     * @throws ValidationException if AUTO_GEN_UUID is not allowed for this key or app.prefix is invalid
     */
    public static void validateAutoGenUuidOptions(String subKey, Map<String, String> tableOptions) {
        if (!(PscConfiguration.PSC_CONSUMER_CLIENT_ID.equals(subKey) || 
              PscConfiguration.PSC_CONSUMER_GROUP_ID.equals(subKey) || 
              PscConfiguration.PSC_PRODUCER_CLIENT_ID.equals(subKey))) {
            throw new ValidationException(
                    String.format("AUTO_GEN_UUID is not allowed for property '%s'. " +
                    "AUTO_GEN_UUID is only supported for: %s, %s, %s", 
                    subKey, 
                    PscConfiguration.PSC_CONSUMER_CLIENT_ID,
                    PscConfiguration.PSC_CONSUMER_GROUP_ID,
                    PscConfiguration.PSC_PRODUCER_CLIENT_ID));
        }
        
        // Validate that client.id.prefix is provided and non-empty when using AUTO_GEN_UUID
        String clientIdPrefix = tableOptions.get("properties.client.id.prefix");
        if (clientIdPrefix == null) {
            throw new ValidationException(
                    "properties.client.id.prefix must be provided when using AUTO_GEN_UUID for ID options");
        }
        
        String trimmedClientIdPrefix = clientIdPrefix.trim();
        if (trimmedClientIdPrefix.isEmpty()) {
            throw new ValidationException(
                    "properties.client.id.prefix must be non-empty (after trimming whitespace) when using AUTO_GEN_UUID for ID options");
        }
    }

    /**
     * Generates a UUID with the client.id.prefix.
     *
     * @param tableOptions the table options map to get client.id.prefix from
     * @return generated UUID string with prefix
     */
    private static String generateUuidWithPrefix(Map<String, String> tableOptions) {
        String clientIdPrefix = tableOptions.get("properties.client.id.prefix");
        String uuid = UUID.randomUUID().toString();
        
        return clientIdPrefix.trim() + "-" + uuid;
    }



    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    public static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateSourceTopic(tableOptions);
        validateScanStartupMode(tableOptions);
        validateScanBoundedMode(tableOptions);
    }

    public static void validateTableSinkOptions(ReadableConfig tableOptions) {
        validateSinkTopic(tableOptions);
        validateSinkPartitioner(tableOptions);
    }

    public static void validateSourceTopic(ReadableConfig tableOptions) {
        Optional<List<String>> topic = tableOptions.getOptional(TOPIC_URI);
        Optional<String> pattern = tableOptions.getOptional(TOPIC_PATTERN);

        if (topic.isPresent() && pattern.isPresent()) {
            throw new ValidationException(
                    "Option 'topic-uri' and 'topic-pattern' shouldn't be set together.");
        }

        if (!topic.isPresent() && !pattern.isPresent()) {
            throw new ValidationException("Either 'topic' or 'topic-pattern' must be set.");
        }
    }

    public static void validateSinkTopic(ReadableConfig tableOptions) {
        String errorMessageTemp =
                "Flink PSC sink currently only supports single topic, but got %s: %s.";
        if (!isSingleTopicUri(tableOptions)) {
            if (tableOptions.getOptional(TOPIC_PATTERN).isPresent()) {
                throw new ValidationException(
                        String.format(
                                errorMessageTemp,
                                "'topic-pattern'",
                                tableOptions.get(TOPIC_PATTERN)));
            } else {
                throw new ValidationException(
                        String.format(errorMessageTemp, "'topic-uri'", tableOptions.get(TOPIC_URI)));
            }
        }
    }

    private static void validateScanStartupMode(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(SCAN_STARTUP_MODE)
                .ifPresent(
                        mode -> {
                            switch (mode) {
                                case TIMESTAMP:
                                    if (!tableOptions
                                            .getOptional(SCAN_STARTUP_TIMESTAMP_MILLIS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' startup mode"
                                                                + " but missing.",
                                                        SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                                                        PscConnectorOptions.ScanStartupMode.TIMESTAMP));
                                    }

                                    break;
                                case SPECIFIC_OFFSETS:
                                    if (!tableOptions
                                            .getOptional(SCAN_STARTUP_SPECIFIC_OFFSETS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' startup mode"
                                                                + " but missing.",
                                                        SCAN_STARTUP_SPECIFIC_OFFSETS.key(),
                                                        PscConnectorOptions.ScanStartupMode.SPECIFIC_OFFSETS));
                                    }
                                    if (!isSingleTopicUri(tableOptions)) {
                                        throw new ValidationException(
                                                "Currently PSC source only supports specific offset for single topic.");
                                    }
                                    String specificOffsets =
                                            tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
                                    parseSpecificOffsets(
                                            specificOffsets, SCAN_STARTUP_SPECIFIC_OFFSETS.key());

                                    break;
                            }
                        });
    }

    static void validateScanBoundedMode(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(SCAN_BOUNDED_MODE)
                .ifPresent(
                        mode -> {
                            switch (mode) {
                                case TIMESTAMP:
                                    if (!tableOptions
                                            .getOptional(SCAN_BOUNDED_TIMESTAMP_MILLIS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' bounded mode"
                                                                + " but missing.",
                                                        SCAN_BOUNDED_TIMESTAMP_MILLIS.key(),
                                                        PscConnectorOptions.ScanBoundedMode.TIMESTAMP));
                                    }

                                    break;
                                case SPECIFIC_OFFSETS:
                                    if (!tableOptions
                                            .getOptional(SCAN_BOUNDED_SPECIFIC_OFFSETS)
                                            .isPresent()) {
                                        throw new ValidationException(
                                                String.format(
                                                        "'%s' is required in '%s' bounded mode"
                                                                + " but missing.",
                                                        SCAN_BOUNDED_SPECIFIC_OFFSETS.key(),
                                                        PscConnectorOptions.ScanBoundedMode.SPECIFIC_OFFSETS));
                                    }
                                    if (!isSingleTopicUri(tableOptions)) {
                                        throw new ValidationException(
                                                "Currently PSC source only supports specific offset for single topicUri.");
                                    }
                                    String specificOffsets =
                                            tableOptions.get(SCAN_BOUNDED_SPECIFIC_OFFSETS);
                                    parseSpecificOffsets(
                                            specificOffsets, SCAN_BOUNDED_SPECIFIC_OFFSETS.key());
                                    break;
                            }
                        });
    }

    private static void validateSinkPartitioner(ReadableConfig tableOptions) {
        tableOptions
                .getOptional(SINK_PARTITIONER)
                .ifPresent(
                        partitioner -> {
                            if (partitioner.equals(SINK_PARTITIONER_VALUE_ROUND_ROBIN)
                                    && tableOptions.getOptional(KEY_FIELDS).isPresent()) {
                                throw new ValidationException(
                                        "Currently 'round-robin' partitioner only works when option 'key.fields' is not specified.");
                            } else if (partitioner.isEmpty()) {
                                throw new ValidationException(
                                        String.format(
                                                "Option '%s' should be a non-empty string.",
                                                SINK_PARTITIONER.key()));
                            }
                        });
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static List<String> getSourceTopicUris(ReadableConfig tableOptions) {
        return tableOptions.getOptional(TOPIC_URI).orElse(null);
    }

    public static Pattern getSourceTopicUriPattern(ReadableConfig tableOptions) {
        return tableOptions.getOptional(TOPIC_PATTERN).map(Pattern::compile).orElse(null);
    }

    private static boolean isSingleTopicUri(ReadableConfig tableOptions) {
        // Option 'topic-pattern' is regarded as multi-topics.
        return tableOptions.getOptional(TOPIC_URI).map(t -> t.size() == 1).orElse(false);
    }

    public static StartupOptions getStartupOptions(ReadableConfig tableOptions) {
        final Map<PscTopicUriPartition, Long> specificOffsets = new HashMap<>();
        final StartupMode startupMode =
                tableOptions
                        .getOptional(SCAN_STARTUP_MODE)
                        .map(PscConnectorOptionsUtil::fromOption)
                        .orElse(StartupMode.GROUP_OFFSETS);
        if (startupMode == StartupMode.SPECIFIC_OFFSETS) {
            // It will be refactored after support specific offset for multiple topics in
            // FLINK-18602. We have already checked tableOptions.get(TOPIC) contains one topic in
            // validateScanStartupMode().
            buildSpecificOffsets(tableOptions, tableOptions.get(TOPIC_URI).get(0), specificOffsets);
        }

        final StartupOptions options = new StartupOptions();
        options.startupMode = startupMode;
        options.specificOffsets = specificOffsets;
        if (startupMode == StartupMode.TIMESTAMP) {
            options.startupTimestampMillis = tableOptions.get(SCAN_STARTUP_TIMESTAMP_MILLIS);
        }
        return options;
    }

    public static BoundedOptions getBoundedOptions(ReadableConfig tableOptions) {
        final Map<PscTopicUriPartition, Long> specificOffsets = new HashMap<>();
        final BoundedMode boundedMode =
                PscConnectorOptionsUtil.fromOption(tableOptions.get(SCAN_BOUNDED_MODE));
        if (boundedMode == BoundedMode.SPECIFIC_OFFSETS) {
            buildBoundedOffsets(tableOptions, tableOptions.get(TOPIC_URI).get(0), specificOffsets);
        }

        final BoundedOptions options = new BoundedOptions();
        options.boundedMode = boundedMode;
        options.specificOffsets = specificOffsets;
        if (boundedMode == BoundedMode.TIMESTAMP) {
            options.boundedTimestampMillis = tableOptions.get(SCAN_BOUNDED_TIMESTAMP_MILLIS);
        }
        return options;
    }

    private static void buildSpecificOffsets(
            ReadableConfig tableOptions,
            String topic,
            Map<PscTopicUriPartition, Long> specificOffsets) {
        String specificOffsetsStrOpt = tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
        final Map<Integer, Long> offsetMap =
                parseSpecificOffsets(specificOffsetsStrOpt, SCAN_STARTUP_SPECIFIC_OFFSETS.key());
        offsetMap.forEach(
                (partition, offset) -> {
                    final PscTopicUriPartition topicPartition =
                            new PscTopicUriPartition(topic, partition);
                    specificOffsets.put(topicPartition, offset);
                });
    }

    public static void buildBoundedOffsets(
            ReadableConfig tableOptions,
            String topic,
            Map<PscTopicUriPartition, Long> specificOffsets) {
        String specificOffsetsEndOpt = tableOptions.get(SCAN_BOUNDED_SPECIFIC_OFFSETS);
        final Map<Integer, Long> offsetMap =
                parseSpecificOffsets(specificOffsetsEndOpt, SCAN_BOUNDED_SPECIFIC_OFFSETS.key());

        offsetMap.forEach(
                (partition, offset) -> {
                    final PscTopicUriPartition topicUriPartition =
                            new PscTopicUriPartition(topic, partition);
                    specificOffsets.put(topicUriPartition, offset);
                });
    }

    /**
     * Returns the {@link StartupMode} of PSC Consumer by passed-in table-specific {@link
     * com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.ScanStartupMode}.
     */
    private static StartupMode fromOption(PscConnectorOptions.ScanStartupMode scanStartupMode) {
        switch (scanStartupMode) {
            case EARLIEST_OFFSET:
                return StartupMode.EARLIEST;
            case LATEST_OFFSET:
                return StartupMode.LATEST;
            case GROUP_OFFSETS:
                return StartupMode.GROUP_OFFSETS;
            case SPECIFIC_OFFSETS:
                return StartupMode.SPECIFIC_OFFSETS;
            case TIMESTAMP:
                return StartupMode.TIMESTAMP;

            default:
                throw new TableException(
                        "Unsupported startup mode. Validator should have checked that.");
        }
    }

    /**
     * Returns the {@link BoundedMode} of PSC Consumer by passed-in table-specific {@link
     * com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptions.ScanBoundedMode}.
     */
    private static BoundedMode fromOption(PscConnectorOptions.ScanBoundedMode scanBoundedMode) {
        switch (scanBoundedMode) {
            case UNBOUNDED:
                return BoundedMode.UNBOUNDED;
            case LATEST_OFFSET:
                return BoundedMode.LATEST;
            case GROUP_OFFSETS:
                return BoundedMode.GROUP_OFFSETS;
            case TIMESTAMP:
                return BoundedMode.TIMESTAMP;
            case SPECIFIC_OFFSETS:
                return BoundedMode.SPECIFIC_OFFSETS;

            default:
                throw new TableException(
                        "Unsupported bounded mode. Validator should have checked that.");
        }
    }

    public static Properties getPscProperties(Map<String, String> tableOptions) {
        final Properties pscProperties = new Properties();

        if (hasPscClientProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey = key.substring((PROPERTIES_PREFIX).length());
                                
                                // Validate and generate UUID for AUTO_GEN_UUID values
                                if (AUTO_GEN_UUID_VALUE.equals(value)) {
                                    validateAutoGenUuidOptions(subKey, tableOptions);
                                    pscProperties.put(subKey, generateUuidWithPrefix(tableOptions));
                                } else {
                                    pscProperties.put(subKey, value);
                                }
                            });
        }
        return pscProperties;
    }

    /**
     * The partitioner can be either "fixed", "round-robin" or a customized partitioner full class
     * name.
     */
    public static Optional<FlinkPscPartitioner<RowData>> getFlinkPscPartitioner(
            ReadableConfig tableOptions, ClassLoader classLoader) {
        return tableOptions
                .getOptional(SINK_PARTITIONER)
                .flatMap(
                        (String partitioner) -> {
                            switch (partitioner) {
                                case SINK_PARTITIONER_VALUE_FIXED:
                                    return Optional.of(new FlinkFixedPartitioner<>());
                                case SINK_PARTITIONER_VALUE_DEFAULT:
                                case SINK_PARTITIONER_VALUE_ROUND_ROBIN:
                                    return Optional.empty();
                                    // Default fallback to full class name of the partitioner.
                                default:
                                    return Optional.of(
                                            initializePartitioner(partitioner, classLoader));
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
            String specificOffsetsStr, String optionKey) {
        final Map<Integer, Long> offsetMap = new HashMap<>();
        final String[] pairs = specificOffsetsStr.split(";");
        final String validationExceptionMessage =
                String.format(
                        "Invalid properties '%s' should follow the format "
                                + "'partition:0,offset:42;partition:1,offset:300', but is '%s'.",
                        optionKey, specificOffsetsStr);

        if (pairs.length == 0) {
            throw new ValidationException(validationExceptionMessage);
        }

        for (String pair : pairs) {
            if (null == pair || pair.length() == 0 || !pair.contains(",")) {
                throw new ValidationException(validationExceptionMessage);
            }

            final String[] kv = pair.split(",");
            if (kv.length != 2
                    || !kv[0].startsWith(PARTITION + ':')
                    || !kv[1].startsWith(OFFSET + ':')) {
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
     * Decides if the table options contains PSC client properties that start with prefix
     * 'properties'.
     */
    private static boolean hasPscClientProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }

    /** Returns a class value with the given class name. */
    private static <T> FlinkPscPartitioner<T> initializePartitioner(
            String name, ClassLoader classLoader) {
        try {
            Class<?> clazz = Class.forName(name, true, classLoader);
            if (!FlinkPscPartitioner.class.isAssignableFrom(clazz)) {
                throw new ValidationException(
                        String.format(
                                "Sink partitioner class '%s' should extend from the required class %s",
                                name, FlinkPscPartitioner.class.getName()));
            }
            @SuppressWarnings("unchecked")
            final FlinkPscPartitioner<T> pscPartitioner =
                    InstantiationUtil.instantiate(name, FlinkPscPartitioner.class, classLoader);

            return pscPartitioner;
        } catch (ClassNotFoundException | FlinkException e) {
            throw new ValidationException(
                    String.format("Could not find and instantiate partitioner class '%s'", name),
                    e);
        }
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the key format and the order that those fields have in the key format.
     *
     * <p>See {@link PscConnectorOptions#KEY_FORMAT}, {@link PscConnectorOptions#KEY_FIELDS},
     * and {@link PscConnectorOptions#KEY_FIELDS_PREFIX} for more information.
     */
    public static int[] createKeyFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final Optional<String> optionalKeyFormat = options.getOptional(KEY_FORMAT);
        final Optional<List<String>> optionalKeyFields = options.getOptional(KEY_FIELDS);

        if (!optionalKeyFormat.isPresent() && optionalKeyFields.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "The option '%s' can only be declared if a key format is defined using '%s'.",
                            KEY_FIELDS.key(), KEY_FORMAT.key()));
        } else if (optionalKeyFormat.isPresent()
                && (!optionalKeyFields.isPresent() || optionalKeyFields.get().size() == 0)) {
            throw new ValidationException(
                    String.format(
                            "A key format '%s' requires the declaration of one or more of key fields using '%s'.",
                            KEY_FORMAT.key(), KEY_FIELDS.key()));
        }

        if (!optionalKeyFormat.isPresent()) {
            return new int[0];
        }

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        final List<String> keyFields = optionalKeyFields.get();
        final List<String> physicalFields = LogicalTypeChecks.getFieldNames(physicalType);
        return keyFields.stream()
                .mapToInt(
                        keyField -> {
                            final int pos = physicalFields.indexOf(keyField);
                            // check that field name exists
                            if (pos < 0) {
                                throw new ValidationException(
                                        String.format(
                                                "Could not find the field '%s' in the table schema for usage in the key format. "
                                                        + "A key field must be a regular, physical column. "
                                                        + "The following columns can be selected in the '%s' option:\n"
                                                        + "%s",
                                                keyField, KEY_FIELDS.key(), physicalFields));
                            }
                            // check that field name is prefixed correctly
                            if (!keyField.startsWith(keyPrefix)) {
                                throw new ValidationException(
                                        String.format(
                                                "All fields in '%s' must be prefixed with '%s' when option '%s' "
                                                        + "is set but field '%s' is not prefixed.",
                                                KEY_FIELDS.key(),
                                                keyPrefix,
                                                KEY_FIELDS_PREFIX.key(),
                                                keyField));
                            }
                            return pos;
                        })
                .toArray();
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the value format.
     *
     * <p>See {@link PscConnectorOptions#VALUE_FORMAT}, {@link
     * PscConnectorOptions#VALUE_FIELDS_INCLUDE}, and {@link
     * PscConnectorOptions#KEY_FIELDS_PREFIX} for more information.
     */
    public static int[] createValueFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);

        final String keyPrefix = options.getOptional(KEY_FIELDS_PREFIX).orElse("");

        final PscConnectorOptions.ValueFieldsStrategy strategy = options.get(VALUE_FIELDS_INCLUDE);
        if (strategy == PscConnectorOptions.ValueFieldsStrategy.ALL) {
            if (keyPrefix.length() > 0) {
                throw new ValidationException(
                        String.format(
                                "A key prefix is not allowed when option '%s' is set to '%s'. "
                                        + "Set it to '%s' instead to avoid field overlaps.",
                                VALUE_FIELDS_INCLUDE.key(),
                                PscConnectorOptions.ValueFieldsStrategy.ALL,
                                PscConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY));
            }
            return physicalFields.toArray();
        } else if (strategy == PscConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY) {
            final int[] keyProjection = createKeyFormatProjection(options, physicalDataType);
            return physicalFields
                    .filter(pos -> IntStream.of(keyProjection).noneMatch(k -> k == pos))
                    .toArray();
        }
        throw new TableException("Unknown value fields strategy:" + strategy);
    }

    /**
     * Returns a new table context with a default schema registry subject value in the options if
     * the format is a schema registry format (e.g. 'avro-confluent') and the subject is not
     * defined.
     */
    public static DynamicTableFactory.Context autoCompleteSchemaRegistrySubject(
            DynamicTableFactory.Context context) {
        Map<String, String> tableOptions = context.getCatalogTable().getOptions();
        Map<String, String> newOptions = autoCompleteSchemaRegistrySubject(tableOptions);
        if (newOptions.size() > tableOptions.size()) {
            // build a new context
            return new FactoryUtil.DefaultDynamicTableContext(
                    context.getObjectIdentifier(),
                    context.getCatalogTable().copy(newOptions),
                    context.getEnrichmentOptions(),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        } else {
            return context;
        }
    }

    private static Map<String, String> autoCompleteSchemaRegistrySubject(
            Map<String, String> options) {
        Configuration configuration = Configuration.fromMap(options);
        // the subject autoComplete should only be used in sink, check the topic first
        validateSinkTopic(configuration);
        final Optional<String> valueFormat = configuration.getOptional(VALUE_FORMAT);
        final Optional<String> keyFormat = configuration.getOptional(KEY_FORMAT);
        final Optional<String> format = configuration.getOptional(FORMAT);
        final String topic = configuration.get(TOPIC_URI).get(0);

        if (format.isPresent() && SCHEMA_REGISTRY_FORMATS.contains(format.get())) {
            autoCompleteSubject(configuration, format.get(), topic + "-value");
        } else if (valueFormat.isPresent() && SCHEMA_REGISTRY_FORMATS.contains(valueFormat.get())) {
            autoCompleteSubject(configuration, "value." + valueFormat.get(), topic + "-value");
        }

        if (keyFormat.isPresent() && SCHEMA_REGISTRY_FORMATS.contains(keyFormat.get())) {
            autoCompleteSubject(configuration, "key." + keyFormat.get(), topic + "-key");
        }
        return configuration.toMap();
    }

    private static void autoCompleteSubject(
            Configuration configuration, String format, String subject) {
        ConfigOption<String> subjectOption =
                ConfigOptions.key(format + "." + SCHEMA_REGISTRY_SUBJECT.key())
                        .stringType()
                        .noDefaultValue();
        if (!configuration.getOptional(subjectOption).isPresent()) {
            configuration.setString(subjectOption, subject);
        }
    }

    static void validateDeliveryGuarantee(ReadableConfig tableOptions) {
        if (tableOptions.get(DELIVERY_GUARANTEE) == DeliveryGuarantee.EXACTLY_ONCE
                && !tableOptions.getOptional(TRANSACTIONAL_ID_PREFIX).isPresent()) {
            throw new ValidationException(
                    TRANSACTIONAL_ID_PREFIX.key()
                            + " must be specified when using DeliveryGuarantee.EXACTLY_ONCE.");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Inner classes
    // --------------------------------------------------------------------------------------------

    /** PSC startup options. * */
    public static class StartupOptions {
        public StartupMode startupMode;
        public Map<PscTopicUriPartition, Long> specificOffsets;
        public long startupTimestampMillis;
    }

    /** PSC bounded options. * */
    public static class BoundedOptions {
        public BoundedMode boundedMode;
        public Map<PscTopicUriPartition, Long> specificOffsets;
        public long boundedTimestampMillis;
    }

    private PscConnectorOptionsUtil() {}
}
