package com.pinterest.psc.producer;

import com.pinterest.psc.common.TestUtils;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.producer.ProducerException;
import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.metrics.NullMetricsReporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class TestTransactionalPscProducer {
    private PscConfiguration pscConfiguration;

    @BeforeEach
    void init() {
        pscConfiguration = new PscConfiguration();
        pscConfiguration.setProperty(PscConfiguration.PSC_PRODUCER_CLIENT_ID, "client-id");
        pscConfiguration.setProperty(PscConfiguration.PSC_CONFIG_LOGGING_ENABLED, "false");
        pscConfiguration.setProperty(PscConfiguration.PSC_METRICS_REPORTER_CLASS, TestUtils.DEFAULT_METRICS_REPORTER);
    }

    /**
     * Validates that PscProducer level state transitions occur as expected
     *
     * @throws ConfigurationException
     */
    @Test
    void testTransactionalStateScenarios() throws ConfigurationException, ProducerException {
        PscProducer<byte[], byte[]> pscProducer = new PscProducer<>(pscConfiguration);
        Exception e;

        assertEquals(PscProducer.TransactionalState.NON_TRANSACTIONAL, pscProducer.getTransactionalState());

        // in NON_TRANSACTIONAL state
        e = assertThrows(ProducerException.class, pscProducer::commitTransaction);
        assertEquals("Invalid transaction state: call to commitTransaction() before initializing transactions.", e.getMessage());

        e = assertThrows(ProducerException.class, pscProducer::abortTransaction);
        assertEquals("Invalid transaction state: call to abortTransaction() before initializing transactions.", e.getMessage());

        // transition to INIT_AND_BEGUN state
        try {
            pscProducer.beginTransaction();
            assertEquals(PscProducer.TransactionalState.INIT_AND_BEGUN, pscProducer.getTransactionalState());
            // ok
        } catch (ProducerException producerException) {
            pscProducer.close();
            fail("Expected beginTransaction() call to succeed!", producerException);
        }

        // in INIT_AND_BEGUN state
        e = assertThrows(ProducerException.class, pscProducer::beginTransaction);
        assertEquals("Invalid transaction state: consecutive calls to beginTransaction().", e.getMessage());
        assertEquals(PscProducer.TransactionalState.NON_TRANSACTIONAL, pscProducer.getTransactionalState());

        // transition to INIT_AND_BEGUN state again
        try {
            pscProducer.beginTransaction();
            assertEquals(PscProducer.TransactionalState.INIT_AND_BEGUN, pscProducer.getTransactionalState());
            // ok
        } catch (ProducerException producerException) {
            pscProducer.close();
            fail("Expected beginTransaction() call to succeed!", producerException);
        }

        // transition back to NON_TRANSACTIONAL state
        try {
            pscProducer.commitTransaction();
            assertEquals(PscProducer.TransactionalState.NON_TRANSACTIONAL, pscProducer.getTransactionalState());
            // ok
        } catch (ProducerException producerException) {
            pscProducer.close();
            fail("Expected commitTransaction() call to succeed!", producerException);
        }

        pscProducer.close();
    }
}
