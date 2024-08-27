package com.pinterest.psc.metadata.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicRn;
import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.metadata.PscBackendMetadataClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class PscKafkaMetadataClient extends PscBackendMetadataClient {

    private AdminClient kafkaAdminClient;

    public void initialize(TopicUri topicUri, ServiceDiscoveryConfig discoveryConfig) {
        super.initialize(topicUri, discoveryConfig);
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, discoveryConfig.getConnect());
        this.kafkaAdminClient = AdminClient.create(properties);
    }

    @Override
    public List<TopicRn> getTopicRns(long timeout, TimeUnit timeUnit) throws ExecutionException, InterruptedException, TimeoutException {
        ListTopicsResult listTopicsResult = kafkaAdminClient.listTopics();
        Collection<TopicListing> topicListing = listTopicsResult.listings().get(timeout, timeUnit);
        return topicListing.stream().map(tl -> getTopicRn(tl.name())).collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {
    }
}
