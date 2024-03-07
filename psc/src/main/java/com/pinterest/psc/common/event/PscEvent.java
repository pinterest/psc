package com.pinterest.psc.common.event;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;

import java.util.Map;

public class PscEvent {
  public final static String EVENT_HEADER = "__EVENT_HEADER";

  private final TopicUri topicUri;
  private final TopicUriPartition topicUriPartition;
  private final String type;
  private final Map<String, Object> context;

  public PscEvent(TopicUri topicUri, TopicUriPartition topicUriPartition, String type,
      Map<String, Object> context) {
    this.topicUri = topicUri;
    this.topicUriPartition = topicUriPartition;
    this.type = type;
    this.context = context;
  }

  public TopicUri getTopicUri() {
    return topicUri;
  }

  public TopicUriPartition getTopicUriPartition() {
    return topicUriPartition;
  }

  public String getType() {
    return type;
  }

  public Map<String, Object> getContext() {
    return context;
  }
}
