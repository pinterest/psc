package com.pinterest.psc.exception;

import com.pinterest.psc.common.TopicUriPartition;

import java.util.Set;
import java.util.stream.Collectors;

public class ExceptionMessage {
    public static final String MULTITHREADED_EXCEPTION = "PscConsumer is not safe for multi-threaded access.";
    public static final String ALREADY_CLOSED_EXCEPTION = "The PSC client instance has already been closed.";
    public static final String ITERATOR_OUT_OF_ELEMENTS = "No more elements exist in the iterator.";
    public static String NO_SUBSCRIPTION_ASSIGNMENT =
            "PSC consumer is not subscribed to the URI or assigned the partition";


    public static String TOPIC_URI_UNSUPPORTED_BACKEND(String backend) {
        return String.format("Unsupported backend in topic Uri: %s", backend);
    }

    public static String NO_SUBSCRIPTION_ASSIGNMENT(String api) {
        return String.format("PSC consumer has no existing subscription or assignment before a call to %s", api);
    }

    public static String MUTUALLY_EXCLUSIVE_APIS(String calledApi, String usedApi) {
        return String.format("%s is not supported when %s is being used.", calledApi, usedApi);
    }

    public static String DUPLICATE_PARTITIONS_IN_MESSAGE_IDS(Set<TopicUriPartition> topicUriPartitions) {
        return String.format("The set of message ids contains duplicate message ids for partition(s): %s",
                String.join(
                        ", ",
                        topicUriPartitions.stream().map(TopicUriPartition::toString).collect(Collectors.toSet())
                )
        );
    }
}
