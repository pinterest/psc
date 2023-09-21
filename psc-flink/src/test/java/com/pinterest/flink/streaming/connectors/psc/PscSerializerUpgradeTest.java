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

package com.pinterest.flink.streaming.connectors.psc;

import com.pinterest.flink.streaming.connectors.psc.internals.FlinkPscInternalProducer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;

/**
 * A {@link TypeSerializerUpgradeTestBase} for  {@link FlinkPscProducer.TransactionStateSerializer}
 * and {@link FlinkPscProducer.ContextStateSerializer}.
 */
//TODO: re-enable when multiple Flink/PSC combinations exist
//See https://github.com/apache/flink/commit/d31a76c128455c1f619f59791a1564ed24b8fa1f for creating snapshots
@RunWith(Parameterized.class)
public class PscSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

    public PscSerializerUpgradeTest(TestSpecification<Object, Object> testSpecification) {
        super(testSpecification);
    }

    @Parameterized.Parameters(name = "Test Specification = {0}")
    public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {

        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
		/*
		for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
			testSpecifications.add(
				new TestSpecification<>(
					"transaction-state-serializer",
					migrationVersion,
					TransactionStateSerializerSetup.class,
					TransactionStateSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"context-state-serializer",
					migrationVersion,
					ContextStateSerializerSetup.class,
					ContextStateSerializerVerifier.class));
		}
		 */
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "transaction-state-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class TransactionStateSerializerSetup implements PreUpgradeSetup<FlinkPscProducer.PscTransactionState> {
        @Override
        public TypeSerializer<FlinkPscProducer.PscTransactionState> createPriorSerializer() {
            return new FlinkPscProducer.TransactionStateSerializer();
        }

        @Override
        public FlinkPscProducer.PscTransactionState createTestData() {
            @SuppressWarnings("unchecked")
            FlinkPscInternalProducer<byte[], byte[]> mock = Mockito.mock(FlinkPscInternalProducer.class);
            return new FlinkPscProducer.PscTransactionState("1234", 3456, (short) 789, mock);
        }
    }

    /**
     * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class TransactionStateSerializerVerifier implements UpgradeVerifier<FlinkPscProducer.PscTransactionState> {
        @Override
        public TypeSerializer<FlinkPscProducer.PscTransactionState> createUpgradedSerializer() {
            return new FlinkPscProducer.TransactionStateSerializer();
        }

        @Override
        public Matcher<FlinkPscProducer.PscTransactionState> testDataMatcher() {
            @SuppressWarnings("unchecked")
            FlinkPscInternalProducer<byte[], byte[]> mock = Mockito.mock(FlinkPscInternalProducer.class);
            return is(new FlinkPscProducer.PscTransactionState("1234", 3456, (short) 789, mock));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<FlinkPscProducer.PscTransactionState>> schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "context-state-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ContextStateSerializerSetup implements PreUpgradeSetup<FlinkPscProducer.PscTransactionContext> {
        @Override
        public TypeSerializer<FlinkPscProducer.PscTransactionContext> createPriorSerializer() {
            return new FlinkPscProducer.ContextStateSerializer();
        }

        @Override
        public FlinkPscProducer.PscTransactionContext createTestData() {
            Set<String> transactionIds = new HashSet<>();
            transactionIds.add("123");
            transactionIds.add("456");
            transactionIds.add("789");
            return new FlinkPscProducer.PscTransactionContext(transactionIds);
        }
    }

    /**
     * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ContextStateSerializerVerifier implements UpgradeVerifier<FlinkPscProducer.PscTransactionContext> {
        @Override
        public TypeSerializer<FlinkPscProducer.PscTransactionContext> createUpgradedSerializer() {
            return new FlinkPscProducer.ContextStateSerializer();
        }

        @Override
        public Matcher<FlinkPscProducer.PscTransactionContext> testDataMatcher() {
            Set<String> transactionIds = new HashSet<>();
            transactionIds.add("123");
            transactionIds.add("456");
            transactionIds.add("789");
            return is(new FlinkPscProducer.PscTransactionContext(transactionIds));
        }

        @Override
        public Matcher<TypeSerializerSchemaCompatibility<FlinkPscProducer.PscTransactionContext>> schemaCompatibilityMatcher(MigrationVersion version) {
            return TypeSerializerMatchers.isCompatibleAsIs();
        }
    }

}
