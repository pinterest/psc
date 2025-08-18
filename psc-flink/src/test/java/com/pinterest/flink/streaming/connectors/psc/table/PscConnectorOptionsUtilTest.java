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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.pinterest.psc.config.PscConfiguration;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptionsUtil.createKeyFormatProjection;
import static com.pinterest.flink.streaming.connectors.psc.table.PscConnectorOptionsUtil.createValueFormatProjection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

/** Test for {@link PscConnectorOptionsUtil}. */
public class PscConnectorOptionsUtilTest {

    @Test
    public void testFormatProjection() {
        final DataType dataType =
                DataTypes.ROW(
                        FIELD("id", INT()),
                        FIELD("name", STRING()),
                        FIELD("age", INT()),
                        FIELD("address", STRING()));

        final Map<String, String> options = createTestOptions();
        options.put("key.fields", "address; name");
        options.put("value.fields-include", "EXCEPT_KEY");

        final Configuration config = Configuration.fromMap(options);

        assertArrayEquals(new int[] {3, 1}, createKeyFormatProjection(config, dataType));
        assertArrayEquals(new int[] {0, 2}, createValueFormatProjection(config, dataType));
    }

    @Test
    public void testMissingKeyFormatProjection() {
        final DataType dataType = ROW(FIELD("id", INT()));
        final Map<String, String> options = createTestOptions();

        final Configuration config = Configuration.fromMap(options);

        try {
            createKeyFormatProjection(config, dataType);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            equalTo(
                                    "A key format 'key.format' requires the declaration of one or more "
                                            + "of key fields using 'key.fields'.")));
        }
    }

    @Test
    public void testInvalidKeyFormatFieldProjection() {
        final DataType dataType = ROW(FIELD("id", INT()), FIELD("name", STRING()));
        final Map<String, String> options = createTestOptions();
        options.put("key.fields", "non_existing");

        final Configuration config = Configuration.fromMap(options);

        try {
            createKeyFormatProjection(config, dataType);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            equalTo(
                                    "Could not find the field 'non_existing' in the table schema for "
                                            + "usage in the key format. A key field must be a regular, "
                                            + "physical column. The following columns can be selected "
                                            + "in the 'key.fields' option:\n"
                                            + "[id, name]")));
        }
    }

    @Test
    public void testInvalidKeyFormatPrefixProjection() {
        final DataType dataType =
                ROW(FIELD("k_part_1", INT()), FIELD("part_2", STRING()), FIELD("name", STRING()));
        final Map<String, String> options = createTestOptions();
        options.put("key.fields", "k_part_1;part_2");
        options.put("key.fields-prefix", "k_");

        final Configuration config = Configuration.fromMap(options);

        try {
            createKeyFormatProjection(config, dataType);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            equalTo(
                                    "All fields in 'key.fields' must be prefixed with 'k_' when option "
                                            + "'key.fields-prefix' is set but field 'part_2' is not prefixed.")));
        }
    }

    @Test
    public void testInvalidValueFormatProjection() {
        final DataType dataType = ROW(FIELD("k_id", INT()), FIELD("id", STRING()));
        final Map<String, String> options = createTestOptions();
        options.put("key.fields", "k_id");
        options.put("key.fields-prefix", "k_");

        final Configuration config = Configuration.fromMap(options);

        try {
            createValueFormatProjection(config, dataType);
            fail();
        } catch (ValidationException e) {
            assertThat(
                    e,
                    hasMessage(
                            equalTo(
                                    "A key prefix is not allowed when option 'value.fields-include' "
                                            + "is set to 'ALL'. Set it to 'EXCEPT_KEY' instead to avoid field overlaps.")));
        }
    }

    // --------------------------------------------------------------------------------------------
    // AUTO_GEN Functionality Tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testAutoGenConstant() {
        // Test that AUTO_GEN constant is correctly defined
        assertEquals("AUTO_GEN", PscConnectorOptionsUtil.AUTO_GEN_VALUE);
    }

    @Test
    public void testGetPscPropertiesWithAutoGenGroupId() {
        // Test AUTO_GEN replacement for group_id
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        assertNotNull("Group ID should not be null", groupId);
        assertNotEquals("Group ID should not be AUTO_GEN", "AUTO_GEN", groupId);

        // Verify it's a valid UUID
        try {
            UUID.fromString(groupId);
        } catch (IllegalArgumentException e) {
            fail("Generated group ID should be a valid UUID: " + groupId);
        }
    }

    @Test
    public void testGetPscPropertiesWithAutoGenClientId() {
        // Test AUTO_GEN replacement for client_id
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_CLIENT_ID, "AUTO_GEN");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String clientId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        assertNotNull("Client ID should not be null", clientId);
        assertNotEquals("Client ID should not be AUTO_GEN", "AUTO_GEN", clientId);

        // Verify it's a valid UUID
        try {
            UUID.fromString(clientId);
        } catch (IllegalArgumentException e) {
            fail("Generated client ID should be a valid UUID: " + clientId);
        }
    }

    @Test
    public void testGetPscPropertiesWithAutoGenProducerClientId() {
        // Test AUTO_GEN replacement for producer client_id
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, "AUTO_GEN");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String producerClientId = pscProperties.getProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID);
        assertNotNull("Producer client ID should not be null", producerClientId);
        assertNotEquals("Producer client ID should not be AUTO_GEN", "AUTO_GEN", producerClientId);

        // Verify it's a valid UUID
        try {
            UUID.fromString(producerClientId);
        } catch (IllegalArgumentException e) {
            fail("Generated producer client ID should be a valid UUID: " + producerClientId);
        }
    }

    @Test
    public void testGetPscPropertiesWithMultipleAutoGen() {
        // Test AUTO_GEN replacement for multiple properties
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN");
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_CLIENT_ID, "AUTO_GEN");
        tableOptions.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, "AUTO_GEN");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String clientId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        String producerClientId = pscProperties.getProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID);

        // All should be valid UUIDs
        assertNotNull("Group ID should not be null", groupId);
        assertNotNull("Client ID should not be null", clientId);
        assertNotNull("Producer Client ID should not be null", producerClientId);
        assertNotEquals("Group ID should not be AUTO_GEN", "AUTO_GEN", groupId);
        assertNotEquals("Client ID should not be AUTO_GEN", "AUTO_GEN", clientId);
        assertNotEquals("Producer Client ID should not be AUTO_GEN", "AUTO_GEN", producerClientId);

        // All should be different UUIDs
        assertNotEquals("Group ID and client ID should be different", groupId, clientId);
        assertNotEquals("Client ID and producer client ID should be different", clientId, producerClientId);
        assertNotEquals("Group ID and producer client ID should be different", groupId, producerClientId);

        // Verify all are valid UUIDs
        try {
            UUID.fromString(groupId);
            UUID.fromString(clientId);
            UUID.fromString(producerClientId);
        } catch (IllegalArgumentException e) {
            fail("All generated IDs should be valid UUIDs");
        }
    }

    @Test
    public void testGetPscPropertiesWithNonAutoGenValues() {
        // Test that non-AUTO_GEN values are preserved
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "my-group");
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_CLIENT_ID, "my-client");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        assertEquals("my-group", pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID));
        assertEquals("my-client", pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID));
    }

    @Test
    public void testGetPscPropertiesWithMixedValues() {
        // Test mixing AUTO_GEN and regular values
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN");
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_CLIENT_ID, "my-client");
        tableOptions.put("properties.some.other.property", "some-value");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        assertNotNull("Group ID should not be null", groupId);
        assertNotEquals("Group ID should not be AUTO_GEN", "AUTO_GEN", groupId);

        // Verify UUID
        try {
            UUID.fromString(groupId);
        } catch (IllegalArgumentException e) {
            fail("Generated group ID should be a valid UUID");
        }

        assertEquals("my-client", pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID));
        assertEquals("some-value", pscProperties.getProperty("some.other.property"));
    }

    @Test
    public void testGetPscPropertiesWithOtherProperties() {
        // Test that other PSC properties are preserved alongside AUTO_GEN
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN");
        tableOptions.put("properties.bootstrap.servers", "localhost:9092");
        tableOptions.put("properties.session.timeout.ms", "30000");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        assertNotNull("Group ID should not be null", groupId);
        assertNotEquals("Group ID should not be AUTO_GEN", "AUTO_GEN", groupId);
        assertEquals("localhost:9092", pscProperties.getProperty("bootstrap.servers"));
        assertEquals("30000", pscProperties.getProperty("session.timeout.ms"));
    }

    @Test
    public void testGetPscPropertiesWithEmptyOptions() {
        // Test with empty options
        Map<String, String> tableOptions = new HashMap<>();

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        assertTrue("Properties should be empty", pscProperties.isEmpty());
    }

    @Test
    public void testGetPscPropertiesWithNoPropertiesPrefix() {
        // Test with options that don't have properties prefix
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("format", "json");
        tableOptions.put("topic", "my-topic");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        assertTrue("Properties should be empty when no properties prefix", pscProperties.isEmpty());
    }

    @Test
    public void testAutoGenGeneratesUniqueUUIDs() {
        // Test that each call to getPscProperties with AUTO_GEN generates unique UUIDs
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN");

        Properties properties1 = PscConnectorOptionsUtil.getPscProperties(tableOptions);
        Properties properties2 = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String groupId1 = properties1.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String groupId2 = properties2.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);

        assertNotNull("Group ID 1 should not be null", groupId1);
        assertNotNull("Group ID 2 should not be null", groupId2);
        assertNotEquals("Each call should generate unique UUIDs", groupId1, groupId2);
    }

    @Test
    public void testGetPscPropertiesPreservesOtherProperties() {
        // Test that AUTO_GEN processing doesn't affect other property handling
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN");
        tableOptions.put("properties.max.poll.records", "1000");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        assertNotNull("Group ID should not be null", groupId);
        assertNotEquals("Group ID should not be AUTO_GEN", "AUTO_GEN", groupId);
        assertEquals("1000", pscProperties.getProperty("max.poll.records"));
    }

    // --------------------------------------------------------------------------------------------
    // Validation and Parsing Tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testParseSpecificOffsetsStillWorks() {
        // Test that existing functionality still works
        String specificOffsets = "partition:0,offset:42;partition:1,offset:300";

        Map<Integer, Long> offsetMap = PscConnectorOptionsUtil.parseSpecificOffsets(specificOffsets, "test-key");

        assertEquals(2, offsetMap.size());
        assertEquals(Long.valueOf(42), offsetMap.get(0));
        assertEquals(Long.valueOf(300), offsetMap.get(1));
    }

    @Test(expected = ValidationException.class)
    public void testParseSpecificOffsetsValidation() {
        // Test that validation still works
        String invalidOffsets = "invalid-format";

        PscConnectorOptionsUtil.parseSpecificOffsets(invalidOffsets, "test-key");
    }

    // --------------------------------------------------------------------------------------------

    private static Map<String, String> createTestOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("key.format", "test-format");
        options.put("key.test-format.delimiter", ",");
        options.put("value.format", "test-format");
        options.put("value.test-format.delimiter", "|");
        options.put("value.test-format.fail-on-missing", "true");
        return options;
    }
}
