package com.pinterest.flink.streaming.connectors.psc.internals.metrics;

public class FlinkPscStateRecoveryMetricConstants {
    // Source
    public static final String PSC_SOURCE_STATE_FRESH = "source.state.fresh.count";
    public static final String PSC_SOURCE_STATE_RECOVERY_KAFKA_SUCCESS = "source.state.recovery.kafka.success.count";
    public static final String PSC_SOURCE_STATE_RECOVERY_KAFKA_FAILURE = "source.state.recovery.kafka.failure.count";
    public static final String PSC_SOURCE_STATE_RECOVERY_KAFKA_NONE = "source.state.recovery.kafka.none.count";
    public static final String PSC_SOURCE_STATE_RECOVERY_PSC_SUCCESS = "source.state.recovery.psc.success.count";
    public static final String PSC_SOURCE_STATE_RECOVERY_PSC_FAILURE = "source.state.recovery.psc.failure.count";
    public static final String PSC_SOURCE_STATE_RECOVERY_PSC_NONE = "source.state.recovery.psc.none.count";

    public static final String PSC_SOURCE_STATE_RECOVERY_KAFKA_PARTITIONS = "source.state.recovery.kafka.partition.count";
    public static final String PSC_SOURCE_STATE_RECOVERY_PSC_PARTITIONS = "source.state.recovery.psc.partition.count";
    public static final String PSC_SOURCE_STATE_SNAPSHOT_PSC_OFFSETS = "source.state.snapshot.psc.offset.count";
    public static final String PSC_SOURCE_STATE_SNAPSHOT_PSC_PENDING_OFFSETS = "source.state.snapshot.psc.pending.offset.count";
    public static final String PSC_SOURCE_STATE_SNAPSHOT_PSC_REMOVED_PENDING_OFFSETS = "source.state.snapshot.psc.removed.pending.offset.count";

    // Sink
    public static final String PSC_SINK_STATE_FRESH = "sink.state.fresh.count";
    public static final String PSC_SINK_STATE_RECOVERY_KAFKA_SUCCESS = "sink.state.recovery.kafka.success.count";
    public static final String PSC_SINK_STATE_RECOVERY_KAFKA_FAILURE = "sink.state.recovery.kafka.failure.count";
    public static final String PSC_SINK_STATE_RECOVERY_PSC_SUCCESS = "sink.state.recovery.psc.success.count";
    public static final String PSC_SINK_STATE_RECOVERY_PSC_FAILURE = "sink.state.recovery.psc.failure.count";

    public static final String PSC_SINK_STATE_RECOVERY_KAFKA_TRANSACTIONS = "sink.state.recovery.kafka.transaction.count";
    public static final String PSC_SINK_STATE_RECOVERY_PSC_TRANSACTIONS = "sink.state.recovery.psc.transaction.count";
    public static final String PSC_SINK_STATE_SNAPSHOT_PSC_PENDING_TRANSACTIONS = "sink.state.snapshot.psc.pending.transaction.count";
    public static final String PSC_SINK_STATE_SNAPSHOT_PSC_STATE_SIZE = "sink.state.snapshot.psc.state.size";
}
