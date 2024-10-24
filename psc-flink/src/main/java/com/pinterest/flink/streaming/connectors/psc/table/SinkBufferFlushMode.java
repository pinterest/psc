package com.pinterest.flink.streaming.connectors.psc.table;

import java.io.Serializable;
import java.util.Objects;

public class SinkBufferFlushMode implements Serializable {
    private static final int DISABLED_BATCH_SIZE = 0;
    private static final long DISABLED_BATCH_INTERVAL = 0L;

    public static final SinkBufferFlushMode DISABLED =
            new SinkBufferFlushMode(DISABLED_BATCH_SIZE, DISABLED_BATCH_INTERVAL);

    private final int batchSize;
    private final long batchIntervalMs;

    public SinkBufferFlushMode(int batchSize, long batchIntervalMs) {
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;

        // validation
        if (isEnabled()
                && !(batchSize > DISABLED_BATCH_SIZE
                && batchIntervalMs > DISABLED_BATCH_INTERVAL)) {
            throw new IllegalArgumentException(
                    String.format(
                            "batchSize and batchInterval must greater than zero if buffer flush is enabled,"
                                    + " but got batchSize=%s and batchIntervalMs=%s",
                            batchSize, batchIntervalMs));
        }
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public boolean isEnabled() {
        return !(batchSize == DISABLED_BATCH_SIZE && batchIntervalMs == DISABLED_BATCH_INTERVAL);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SinkBufferFlushMode that = (SinkBufferFlushMode) o;
        return batchSize == that.batchSize && batchIntervalMs == that.batchIntervalMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchSize, batchIntervalMs);
    }
}
