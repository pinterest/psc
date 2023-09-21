package com.pinterest.psc.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.PscUtils;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.config.PscConfigurationInternal;
import com.pinterest.psc.config.PscConfigurationReporter;
import com.pinterest.psc.environment.Environment;
import com.pinterest.psc.logging.PscLogger;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Encapsulation of logic used in building and maintaining tags in PSC's metrics via singleton approach
 */
public class PscMetricTagManager {
    private static final PscLogger logger = PscLogger.getLogger(PscMetricTagManager.class);
    private static final PscMetricTagManager singletonPscMetricTagManager = new PscMetricTagManager();
    private static final String pscVersion = PscUtils.getVersion();
    private final Map<MetricTagKey, PscMetricTag> pscBaseTagsMap = new ConcurrentHashMap<>();
    private final Map<MetricTagKey, PscMetricTag> pscMetricTagsMap = new ConcurrentHashMap<>();
    private Environment environment;
    private final String processId;

    private PscMetricTagManager() {
        processId = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    }

    @VisibleForTesting
    protected void cleanup() {
        pscBaseTagsMap.clear();
        pscMetricTagsMap.clear();
    }

    public static PscMetricTagManager getInstance() {
        return singletonPscMetricTagManager;
    }

    public void initializePscMetricTagManager(PscConfigurationInternal pscConfigurationInternal) {
        if (pscConfigurationInternal == null) {
            throw new IllegalArgumentException("Cannot initialize PscMetricTagManager with null pscConfigurationInternal");
        }
        this.environment = pscConfigurationInternal.getEnvironment();
        logger.info("Initialized PscMetricTagManager in thread {}", Thread.currentThread().getId());
    }

    /**
     * Given the combination of {@link TopicUri}, partition, and {@link PscConfigurationInternal}, return the pre-filled
     * {@link PscMetricTag} if it already exists or create it.
     * @param topicUri
     * @param partition
     * @param pscConfigurationInternal
     * @return {@link PscMetricTag} pre-filled with necessary tags
     */
    public PscMetricTag getOrCreatePscMetricTag(TopicUri topicUri, int partition, PscConfigurationInternal pscConfigurationInternal) {
        String topicUriStr = topicUri == null ? PscUtils.NO_TOPIC_URI : topicUri.getTopicUriAsString();
        MetricTagKey pscMetricTagKey = new MetricTagKey(topicUriStr, partition);

        if (pscMetricTagsMap.containsKey(pscMetricTagKey)) {
            PscMetricTag metricTags = pscMetricTagsMap.get(pscMetricTagKey);
            return metricTags;
        }

        if (pscConfigurationInternal == null || pscConfigurationInternal.getConfiguration() == null) {
            logger.error("PscMetricRegistryManager is not initialized yet.");
            return null;
        }

        // skip any metric reporting for the internal config logging producer
        if (PscConfigurationReporter.isThisYou(pscConfigurationInternal.getConfiguration()))
            return null;


        PscMetricTag.Builder builder = getBaseMetricTagBuilder(pscConfigurationInternal).id(topicUriStr).threadId(Thread.currentThread().getId());
        if (topicUri != null)
            builder.tags(topicUri.getComponents());

        if (pscConfigurationInternal.getClientType().equals(PscConfiguration.PSC_CLIENT_TYPE_CONSUMER)) {
            builder.tag(PscMetricTag.PSC_TAG_GROUP, pscConfigurationInternal.getPscConsumerGroupId())
                    .tag(PscMetricTag.PSC_TAG_CLIENT_ID, pscConfigurationInternal.getPscConsumerClientId());
        } else if (pscConfigurationInternal.getClientType().equals(PscConfiguration.PSC_CLIENT_TYPE_PRODUCER)) {
            builder.tag(PscMetricTag.PSC_TAG_CLIENT_ID, pscConfigurationInternal.getPscProducerClientId());
        }

        builder.tag(PscMetricTag.PSC_CLIENT_TYPE, pscConfigurationInternal.getClientType());

        if (partition >= 0)
            builder.tag(PscMetricTag.PSC_TAG_PARTITION, "" + partition);

        PscMetricTag pscMetricTag = builder.build();
        pscMetricTagsMap.put(pscMetricTagKey, pscMetricTag);
        return pscMetricTag;
    }

    public PscMetricTag getOrCreateBaseMetricTag(String id, long threadId, PscConfigurationInternal pscConfigurationInternal) {
        if (pscConfigurationInternal == null || pscConfigurationInternal.getConfiguration() == null) {
            logger.error("PscMetricRegistryManager is not initialized yet.");
            return null;
        }

        MetricTagKey metricTagKey = new MetricTagKey(id, threadId);

        if (pscBaseTagsMap.containsKey(metricTagKey))
            return pscBaseTagsMap.get(metricTagKey);

        PscMetricTag pscMetricTag = getBaseMetricTagBuilder(pscConfigurationInternal)
                .id(id)
                .threadId(threadId)
                .build();

        pscBaseTagsMap.put(metricTagKey, pscMetricTag);
        return pscMetricTag;
    }

    private PscMetricTag.Builder getBaseMetricTagBuilder(PscConfigurationInternal pscConfigurationInternal) {
        return new PscMetricTag.Builder()
                .hostname(environment.getHostname())
                .hostIp(environment.getIpAddress())
                .processId(processId)
                .project(pscConfigurationInternal.getProject())
                .locality(environment.getLocality())
                .instanceType(environment.getInstanceType())
                .version(pscVersion);
    }

    static class MetricTagKey {
        private final String topicUriStr;
        private int partition = PscUtils.NO_PARTITION;
        private long threadId = -1;

        public MetricTagKey(String topicUriStr, int partition) {
            this.topicUriStr = topicUriStr;
            this.partition = partition;
        }

        public MetricTagKey(String topicUriStr, long threadId) {
            this.topicUriStr = topicUriStr;
            this.threadId = threadId;
        }

        public MetricTagKey(String topicUriStr, int partition, long threadId) {
            this(topicUriStr, threadId);
            this.partition = partition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MetricTagKey that = (MetricTagKey) o;

            if (partition != that.partition) return false;
            if (threadId != that.threadId) return false;
            return topicUriStr != null ? topicUriStr.equals(that.topicUriStr) : that.topicUriStr == null;
        }

        @Override
        public int hashCode() {
            int result = topicUriStr != null ? topicUriStr.hashCode() : 0;
            result = 31 * result + partition;
            result = 31 * result + (int) (threadId ^ (threadId >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return String.format("topicUriStr=%s, partition=%d, threadId=%d", topicUriStr, partition, threadId);
        }
    }
}
