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

import com.pinterest.psc.config.PscConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link PscConnectorOptions} ConfigOptions definitions. */
public class PscConnectorOptionsTest {

    @Test
    public void testConfigOptionsDefaultValues() {
        // Test that client_id ConfigOptions have AUTO_GEN as default values
        // group_id is now required (no default value) but supports AUTO_GEN when explicitly set
        assertNull(
                "Group ID should have no default value (required)",
                PscConnectorOptions.PROPS_GROUP_ID.defaultValue());
        assertEquals("AUTO_GEN", PscConnectorOptions.PROPS_CLIENT_ID.defaultValue());
        assertEquals("AUTO_GEN", PscConnectorOptions.PROPS_PRODUCER_CLIENT_ID.defaultValue());
    }

    @Test
    public void testConfigOptionsKeys() {
        // Test that ConfigOptions have correct keys
        assertEquals("properties." + PscConfiguration.PSC_CONSUMER_GROUP_ID, PscConnectorOptions.PROPS_GROUP_ID.key());
        assertEquals("properties." + PscConfiguration.PSC_CONSUMER_CLIENT_ID, PscConnectorOptions.PROPS_CLIENT_ID.key());
        assertEquals("properties." + PscConfiguration.PSC_PRODUCER_CLIENT_ID, PscConnectorOptions.PROPS_PRODUCER_CLIENT_ID.key());
    }

    @Test
    public void testGroupIdMandatoryConfiguration() {
        // Test that group_id has no default value (making it mandatory)
        assertNull("Group ID should have no default value", PscConnectorOptions.PROPS_GROUP_ID.defaultValue());

        // But description should indicate AUTO_GEN is supported
        String description = PscConnectorOptions.PROPS_GROUP_ID.description().toString();
        assertTrue("Description should mention AUTO_GEN support", description.contains("AUTO_GEN"));
        assertTrue("Description should mention it's required", description.contains("Required"));
    }

    @Test
    public void testClientIdDefaultsToAutoGen() {
        // Test that client IDs default to AUTO_GEN for backward compatibility
        assertEquals("Consumer client ID should default to AUTO_GEN",
                "AUTO_GEN", PscConnectorOptions.PROPS_CLIENT_ID.defaultValue());
        assertEquals("Producer client ID should default to AUTO_GEN",
                "AUTO_GEN", PscConnectorOptions.PROPS_PRODUCER_CLIENT_ID.defaultValue());
    }

    @Test
    public void testConfigOptionDescriptions() {
        // Test that ConfigOptions have meaningful descriptions
        assertNotNull("Group ID should have description", PscConnectorOptions.PROPS_GROUP_ID.description());
        assertNotNull("Client ID should have description", PscConnectorOptions.PROPS_CLIENT_ID.description());
        assertNotNull("Producer Client ID should have description", PscConnectorOptions.PROPS_PRODUCER_CLIENT_ID.description());

        // Test specific description content
        assertTrue("Group ID description should mention requirement",
                PscConnectorOptions.PROPS_GROUP_ID.description().toString().contains("Required"));
        assertTrue("Client ID description should mention AUTO_GEN",
                PscConnectorOptions.PROPS_CLIENT_ID.description().toString().contains("AUTO_GEN"));
        assertTrue("Producer Client ID description should mention AUTO_GEN",
                PscConnectorOptions.PROPS_PRODUCER_CLIENT_ID.description().toString().contains("AUTO_GEN"));
    }
}
