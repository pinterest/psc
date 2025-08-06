package com.pinterest.psc.producer.transaction;

import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestTransactionManagerUtils {

    @Test
    void testKafkaTransactionManagerOperator() {
        // create a Kafka TransactionManager just for testing
        TransactionManager transactionManager = new TransactionManager(new LogContext(), null, 10000, 100L, null);

        long id = TransactionManagerUtils.getProducerId(transactionManager);
        short epoch = TransactionManagerUtils.getEpoch(transactionManager);
        String transactionId = TransactionManagerUtils.getTransactionId(transactionManager);
        assertEquals(-1, id);   // uninitialized
        assertEquals(-1, epoch);   // uninitialized
        assertNull(transactionId);   // uninitialized

        TransactionManagerUtils.setProducerId(transactionManager, 100L);
        TransactionManagerUtils.setEpoch(transactionManager, (short) 1);
        TransactionManagerUtils.setTransactionId(transactionManager, "transaction-id");
        assertEquals(100L, TransactionManagerUtils.getProducerId(transactionManager));
        assertEquals(1, TransactionManagerUtils.getEpoch(transactionManager));
        assertEquals("transaction-id", TransactionManagerUtils.getTransactionId(transactionManager));
    }
}
