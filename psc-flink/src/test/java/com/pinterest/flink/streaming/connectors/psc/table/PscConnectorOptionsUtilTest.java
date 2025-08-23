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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
    // AUTO_GEN_UUID Functionality Tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testAutoGenUuidConstant() {
        // Test that AUTO_GEN_UUID constant is correctly defined
        assertEquals("AUTO_GEN_UUID", PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);
    }

    @Test
    public void testGetPscPropertiesWithAutoGenUuidGroupId() {
        // Test AUTO_GEN_UUID replacement for group_id with client.id.prefix
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
        tableOptions.put("properties.client.id.prefix", "test-app");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        assertNotNull("Group ID should not be null", groupId);
        assertNotEquals("Group ID should not be AUTO_GEN_UUID", "AUTO_GEN_UUID", groupId);
        assertTrue("Group ID should start with client.id.prefix", groupId.startsWith("test-app-"));

        // Verify the suffix after prefix is a valid UUID
        String uuidPart = groupId.substring("test-app-".length());
        try {
            UUID.fromString(uuidPart);
        } catch (IllegalArgumentException e) {
            fail("Generated group ID suffix should be a valid UUID: " + uuidPart);
        }
    }

    @Test
    public void testGetPscPropertiesWithoutConsumerClientId() {
        // Test that missing consumer client ID is not set (handled by PscTopicUriPartitionSplitReader)
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties.client.id.prefix", "test-client");
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        // Consumer client ID should not be present - it's handled elsewhere
        String clientId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        assertEquals("Consumer client ID should not be set in properties", null, clientId);
        // Other properties should be present
        assertEquals("test-group", pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID));
    }

    @Test
    public void testGetPscPropertiesWithAutoGenProducerClientId() {
        // Test AUTO_GEN_UUID replacement for producer client_id
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);
        tableOptions.put("properties.client.id.prefix", "test-producer");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String producerClientId = pscProperties.getProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID);
        assertNotNull("Producer client ID should not be null", producerClientId);
        assertNotEquals("Producer client ID should not be AUTO_GEN_UUID", PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE, producerClientId);
        assertTrue("Producer client ID should start with prefix", producerClientId.startsWith("test-producer-"));

        // Verify the suffix is a valid UUID
        String uuidPart = producerClientId.substring("test-producer-".length());
        try {
            UUID.fromString(uuidPart);
        } catch (IllegalArgumentException e) {
            fail("Generated producer client ID suffix should be a valid UUID: " + uuidPart);
        }
    }

    @Test
    public void testGetPscPropertiesWithMultipleAutoGen() {
        // Test AUTO_GEN_UUID replacement for multiple properties (group ID + producer ID)
        // Consumer client ID is handled by PscTopicUriPartitionSplitReader
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);
        tableOptions.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);
        tableOptions.put("properties.client.id.prefix", "multi-test");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String clientId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        String producerClientId = pscProperties.getProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID);

        // Group ID and Producer ID should be valid prefixed UUIDs
        assertNotNull("Group ID should not be null", groupId);
        assertEquals("Consumer client ID should not be set", null, clientId);  // Handled elsewhere
        assertNotNull("Producer Client ID should not be null", producerClientId);
        assertNotEquals("Group ID should not be AUTO_GEN_UUID", PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE, groupId);
        assertNotEquals("Producer Client ID should not be AUTO_GEN_UUID", PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE, producerClientId);

        // Group ID and Producer Client ID should start with prefix
        assertTrue("Group ID should start with prefix", groupId.startsWith("multi-test-"));
        assertTrue("Producer Client ID should start with prefix", producerClientId.startsWith("multi-test-"));

        // Group ID and producer client ID should be different UUIDs
        assertNotEquals("Group ID and producer client ID should be different", groupId, producerClientId);

        // Verify suffix parts are valid UUIDs
        try {
            String groupUuidPart = groupId.substring("multi-test-".length());
            String producerUuidPart = producerClientId.substring("multi-test-".length());
            UUID.fromString(groupUuidPart);
            UUID.fromString(producerUuidPart);
        } catch (IllegalArgumentException e) {
            fail("All generated ID suffixes should be valid UUIDs");
        }
    }

    @Test
    public void testGetPscPropertiesWithNonAutoGenValues() {
        // Test that non-AUTO_GEN values are preserved (consumer client ID handled elsewhere)
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "my-group");
        tableOptions.put("properties.client.id.prefix", "non-auto");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        assertEquals("my-group", pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID));
        // Consumer client ID should not be set (handled by PscTopicUriPartitionSplitReader)
        String clientId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        assertEquals("Consumer client ID should not be set", null, clientId);
    }

    @Test
    public void testGetPscPropertiesWithMixedValues() {
        // Test mixing AUTO_GEN_UUID and regular values (consumer client ID handled elsewhere)
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);
        tableOptions.put("properties.some.other.property", "some-value");
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "mixed-test");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        assertNotNull("Group ID should not be null", groupId);
        assertNotEquals("Group ID should not be AUTO_GEN_UUID", PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE, groupId);
        assertTrue("Group ID should start with prefix", groupId.startsWith("mixed-test-"));

        // Verify UUID suffix
        String uuidPart = groupId.substring("mixed-test-".length());
        try {
            UUID.fromString(uuidPart);
        } catch (IllegalArgumentException e) {
            fail("Generated group ID suffix should be a valid UUID");
        }

        // Consumer client ID should not be set in properties (handled by PscTopicUriPartitionSplitReader)
        String clientId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        assertEquals("Consumer client ID should not be set", null, clientId);
        assertEquals("some-value", pscProperties.getProperty("some.other.property"));
    }

    @Test
    public void testGetPscPropertiesWithOtherProperties() {
        // Test that other PSC properties are preserved alongside AUTO_GEN_UUID
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);
        tableOptions.put("properties.bootstrap.servers", "localhost:9092");
        tableOptions.put("properties.session.timeout.ms", "30000");
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "other-test");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        assertNotNull("Group ID should not be null", groupId);
        assertNotEquals("Group ID should not be AUTO_GEN_UUID", PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE, groupId);
        assertTrue("Group ID should start with prefix", groupId.startsWith("other-test-"));
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
        // Test that each call to getPscProperties with AUTO_GEN_UUID generates unique UUIDs
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "unique-test");

        Properties properties1 = PscConnectorOptionsUtil.getPscProperties(tableOptions);
        Properties properties2 = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String groupId1 = properties1.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        String groupId2 = properties2.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);

        assertNotNull("Group ID 1 should not be null", groupId1);
        assertNotNull("Group ID 2 should not be null", groupId2);
        assertNotEquals("Each call should generate unique UUIDs", groupId1, groupId2);
        assertTrue("Group ID 1 should start with prefix", groupId1.startsWith("unique-test-"));
        assertTrue("Group ID 2 should start with prefix", groupId2.startsWith("unique-test-"));
    }

    @Test
    public void testGetPscPropertiesPreservesOtherProperties() {
        // Test that AUTO_GEN_UUID processing doesn't affect other property handling
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);
        tableOptions.put("properties.max.poll.records", "1000");
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "preserve-test");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        assertNotNull("Group ID should not be null", groupId);
        assertNotEquals("Group ID should not be AUTO_GEN_UUID", PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE, groupId);
        assertTrue("Group ID should start with prefix", groupId.startsWith("preserve-test-"));
        assertEquals("1000", pscProperties.getProperty("max.poll.records"));
    }

    // --------------------------------------------------------------------------------------------
    // AUTO_GEN Validation Tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testValidateAutoGenUuidOptionsWithAllowedKeys() {
        // Test that the function does not throw for allowed keys with valid client.id.prefix
        // Note: Consumer client ID is no longer allowed with AUTO_GEN_UUID - it's auto-generated when missing
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-prefix");
        try {
            // Only group ID and producer client ID are allowed with AUTO_GEN_UUID
            PscConnectorOptionsUtil.validateAutoGenUuidOptions(PscConfiguration.PSC_CONSUMER_GROUP_ID, tableOptions);
            PscConnectorOptionsUtil.validateAutoGenUuidOptions(PscConfiguration.PSC_PRODUCER_CLIENT_ID, tableOptions);
            // If we get here, all validations passed
        } catch (ValidationException e) {
            fail("Validation should not fail for allowed keys: " + e.getMessage());
        }
    }

    @Test
    public void testValidateAutoGenUuidOptionsWithNonAllowedKeys() {
        // Test that the function throws ValidationException for non-allowed keys
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-prefix");
        String[] nonAllowedKeys = {
            "bootstrap.servers",
            "session.timeout.ms",
            "max.poll.records",
            "random.property",
            PscConfiguration.PSC_CONSUMER_CLIENT_ID,  // Consumer client ID no longer allowed with AUTO_GEN_UUID
            ""
        };
        for (String key : nonAllowedKeys) {
            try {
                PscConnectorOptionsUtil.validateAutoGenUuidOptions(key, tableOptions);
                fail("Should have thrown ValidationException for key: " + key);
            } catch (ValidationException e) {
                // Expected - verify error message contains the key and mentions allowed keys
                String message = e.getMessage();
                assertTrue("Error message should mention the invalid key", message.contains(key));
                assertTrue("Error message should mention AUTO_GEN_UUID", message.contains("AUTO_GEN_UUID"));
                assertTrue("Error message should list allowed keys", message.contains(PscConfiguration.PSC_CONSUMER_GROUP_ID));
            }
        }
    }

    @Test(expected = ValidationException.class)
    public void testValidateAutoGenUuidOptionsWithNull() {
        // Test that null throws ValidationException
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-prefix");
        PscConnectorOptionsUtil.validateAutoGenUuidOptions(null, tableOptions);
    }

    @Test(expected = ValidationException.class)
    public void testGetPscPropertiesWithAutoGenOnNonAllowedKey() {
        // Test that using AUTO_GEN_UUID with non-allowed keys throws ValidationException
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties.bootstrap.servers", PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test");

        PscConnectorOptionsUtil.getPscProperties(tableOptions);
    }

    @Test(expected = ValidationException.class)
    public void testGetPscPropertiesWithAutoGenOnAnotherNonAllowedKey() {
        // Test with another non-allowed key
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties.session.timeout.ms", PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test");

        PscConnectorOptionsUtil.getPscProperties(tableOptions);
    }

    @Test
    public void testGetPscPropertiesValidationMessage() {
        // Test that the validation error message is informative
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties.invalid.key", PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test");

        try {
            PscConnectorOptionsUtil.getPscProperties(tableOptions);
            fail("Should have thrown ValidationException");
        } catch (ValidationException e) {
            String message = e.getMessage();
            assertTrue("Error message should mention the invalid key", message.contains("invalid.key"));
            assertTrue("Error message should mention AUTO_GEN_UUID", message.contains("AUTO_GEN_UUID"));
            assertTrue("Error message should list allowed keys", message.contains(PscConfiguration.PSC_CONSUMER_GROUP_ID));
            assertTrue("Error message should list allowed keys", message.contains(PscConfiguration.PSC_PRODUCER_CLIENT_ID));
        }
    }

    @Test
    public void testGetPscPropertiesWithMixedValidAndInvalidAutoGen() {
        // Test mixing allowed AUTO_GEN_UUID with non-allowed AUTO_GEN_UUID
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);  // This should work
        tableOptions.put("properties.invalid.key", PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);  // This should fail
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "mixed");

        try {
            PscConnectorOptionsUtil.getPscProperties(tableOptions);
            fail("Should have thrown ValidationException for invalid.key");
        } catch (ValidationException e) {
            assertTrue("Error should be about the invalid key", e.getMessage().contains("invalid.key"));
        }
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

    @Test
    public void testAutoGenUuidRequiresClientIdPrefix() {
        // Test that AUTO_GEN_UUID requires client.id.prefix
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
        // Intentionally not setting client.id.prefix

        try {
            PscConnectorOptionsUtil.getPscProperties(tableOptions);
            fail("Should have thrown ValidationException for missing client.id.prefix");
        } catch (ValidationException e) {
            String message = e.getMessage();
            assertTrue("Error message should mention client.id.prefix requirement",
                    message.contains("properties.client.id.prefix") && message.contains("must be provided"));
        }
    }

    @Test
    public void testAutoGenUuidWithEmptyClientIdPrefix() {
        // Test that AUTO_GEN_UUID requires non-empty client.id.prefix
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "");

        try {
            PscConnectorOptionsUtil.getPscProperties(tableOptions);
            fail("Should have thrown ValidationException for empty client.id.prefix");
        } catch (ValidationException e) {
            String message = e.getMessage();
            assertTrue("Error message should mention client.id.prefix requirement",
                    message.contains("properties.client.id.prefix") && message.contains("must be non-empty"));
        }
    }

    @Test
    public void testAutoGenUuidWithWhitespaceOnlyClientIdPrefix() {
        // Test that AUTO_GEN_UUID requires non-empty client.id.prefix after trimming
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "   ");  // Only whitespace

        try {
            PscConnectorOptionsUtil.getPscProperties(tableOptions);
            fail("Should have thrown ValidationException for whitespace-only client.id.prefix");
        } catch (ValidationException e) {
            String message = e.getMessage();
            assertTrue("Error message should mention trimming whitespace",
                    message.contains("after trimming whitespace"));
        }
    }

    @Test
    public void testAutoGenUuidWithTrimmableClientIdPrefix() {
        // Test that AUTO_GEN_UUID properly trims client.id.prefix whitespace
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "  my-app  ");  // Leading and trailing whitespace

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);
        assertNotNull("Group ID should not be null", groupId);
        assertNotEquals("Group ID should not be AUTO_GEN_UUID", "AUTO_GEN_UUID", groupId);
        assertTrue("Group ID should start with trimmed prefix", groupId.startsWith("my-app-"));
        assertFalse("Group ID should not have leading whitespace", groupId.startsWith(" "));
        assertFalse("Group ID should not have trailing whitespace in prefix", groupId.contains("  -"));

        // Verify the UUID part after the prefix is valid
        String uuidPart = groupId.substring("my-app-".length());
        try {
            UUID.fromString(uuidPart);
        } catch (IllegalArgumentException e) {
            fail("Generated group ID suffix should be a valid UUID: " + uuidPart);
        }
    }

    // --------------------------------------------------------------------------------------------
    // New Validation Methods Tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testValidateConsumerClientOptionsSuccess() {
        // Test that validateConsumerClientOptions passes with valid configuration
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group");
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-prefix");
        Configuration config = Configuration.fromMap(tableOptions);
        try {
            PscConnectorOptionsUtil.validateConsumerClientOptions(config);
            // Should not throw exception
        } catch (ValidationException e) {
            fail("Should not fail with valid consumer client options: " + e.getMessage());
        }
    }

    @Test(expected = ValidationException.class)
    public void testValidateConsumerClientOptionsWithMissingGroupId() {
        // Test that validateConsumerClientOptions fails when group ID is missing
        Map<String, String> tableOptions = new HashMap<>();
        // Missing group ID
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-prefix");
        Configuration config = Configuration.fromMap(tableOptions);
        PscConnectorOptionsUtil.validateConsumerClientOptions(config);
    }

    @Test(expected = ValidationException.class)
    public void testValidateConsumerClientOptionsWithMissingClientIdPrefix() {
        // Test that validateConsumerClientOptions fails when client.id.prefix is missing
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group");
        // Missing client.id.prefix
        Configuration config = Configuration.fromMap(tableOptions);
        PscConnectorOptionsUtil.validateConsumerClientOptions(config);
    }

    @Test
    public void testValidateProducerClientOptionsSuccess() {
        // Test that validateProducerClientOptions passes with valid configuration
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, "test-producer");
        Configuration config = Configuration.fromMap(tableOptions);
        try {
            PscConnectorOptionsUtil.validateProducerClientOptions(config);
            // Should not throw exception
        } catch (ValidationException e) {
            fail("Should not fail with valid producer client options: " + e.getMessage());
        }
    }

    @Test(expected = ValidationException.class)
    public void testValidateProducerClientOptionsWithMissingProducerId() {
        // Test that validateProducerClientOptions fails when producer client ID is missing
        Map<String, String> tableOptions = new HashMap<>();
        // Missing producer client ID
        Configuration config = Configuration.fromMap(tableOptions);
        PscConnectorOptionsUtil.validateProducerClientOptions(config);
    }

    @Test
    public void testValidateProducerClientOptionsWithAutoGenUuid() {
        // Test that validateProducerClientOptions works with AUTO_GEN_UUID
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, PscConnectorOptionsUtil.AUTO_GEN_UUID_VALUE);
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-producer");
        Configuration config = Configuration.fromMap(tableOptions);
        try {
            PscConnectorOptionsUtil.validateProducerClientOptions(config);
            // Should not throw exception
        } catch (ValidationException e) {
            fail("Should not fail with AUTO_GEN_UUID producer client options: " + e.getMessage());
        }
    }

    @Test
    public void testGetPscPropertiesDoesNotSetConsumerClientId() {
        // Test that consumer client ID is not set in properties (handled by PscTopicUriPartitionSplitReader)
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "test-group");
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "test-prefix");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);

        String clientId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_CLIENT_ID);
        assertEquals("Consumer client ID should not be set in properties", null, clientId);
        // Other properties should be present
        assertEquals("test-group", pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID));
    }

    @Test
    public void testClientIdPrefixAlias() {
        // Test that properties.psc.consumer.client.id works as an alias for properties.client.id.prefix
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
        // Use alias instead of primary key
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_CLIENT_ID, "test-alias-prefix");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);
        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);

        // Verify AUTO_GEN_UUID was processed with the alias prefix
        assertNotNull("Generated group ID should not be null", groupId);
        assertNotEquals("Generated group ID should not equal AUTO_GEN_UUID", "AUTO_GEN_UUID", groupId);
        assertTrue("Generated group ID should start with alias prefix", groupId.startsWith("test-alias-prefix-"));

        // Verify the UUID part is valid
        String uuidPart = groupId.substring("test-alias-prefix-".length());
        try {
            UUID.fromString(uuidPart);
            // If no exception is thrown, the UUID is valid
        } catch (IllegalArgumentException e) {
            fail("Generated group ID suffix should be a valid UUID: " + uuidPart);
        }
    }

    @Test
    public void testClientIdPrefixPrimaryTakesPrecedence() {
        // Test that properties.client.id.prefix takes precedence over the alias when both are provided
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
        // Provide both primary key and alias
        tableOptions.put(PscConnectorOptions.PROPS_CLIENT_ID_PREFIX.key(), "primary-prefix");
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_CLIENT_ID, "alias-prefix");

        Properties pscProperties = PscConnectorOptionsUtil.getPscProperties(tableOptions);
        String groupId = pscProperties.getProperty(PscConfiguration.PSC_CONSUMER_GROUP_ID);

        // Verify the primary key prefix was used, not the alias
        assertNotNull("Generated group ID should not be null", groupId);
        assertTrue("Generated group ID should start with primary prefix", groupId.startsWith("primary-prefix-"));
        assertFalse("Generated group ID should not start with alias prefix", groupId.startsWith("alias-prefix-"));
    }

    @Test
    public void testClientIdPrefixAliasValidationError() {
        // Test that validation error mentions both primary key and alias when neither is provided
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
        // Don't provide either primary key or alias

        try {
            PscConnectorOptionsUtil.getPscProperties(tableOptions);
            fail("Expected ValidationException to be thrown");
        } catch (ValidationException e) {
            // Verify the error message mentions both the primary key and alias
            String message = e.getMessage();
            assertTrue("Error message should mention primary key",
                       message.contains("properties.client.id.prefix"));
            assertTrue("Error message should mention alias",
                       message.contains("properties.psc.consumer.client.id"));
        }
    }

    @Test
    public void testClientIdPrefixAliasEmptyValidationError() {
        // Test that validation error occurs when alias is provided but empty
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, "AUTO_GEN_UUID");
        // Provide empty alias
        tableOptions.put("properties." + PscConfiguration.PSC_CONSUMER_CLIENT_ID, "   ");

        try {
            PscConnectorOptionsUtil.getPscProperties(tableOptions);
            fail("Expected ValidationException to be thrown");
        } catch (ValidationException e) {
            // Verify the error message mentions non-empty requirement for both primary key and alias
            String message = e.getMessage();
            assertTrue("Error message should mention non-empty requirement",
                       message.contains("must be non-empty"));
            assertTrue("Error message should mention primary key",
                       message.contains("properties.client.id.prefix"));
            assertTrue("Error message should mention alias",
                       message.contains("properties.psc.consumer.client.id"));
        }
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
