package com.pinterest.psc.common.event;

import com.pinterest.psc.common.TopicUri;
import com.pinterest.psc.common.TopicUriPartition;

import java.util.Map;

/**
 * PscEvent framework enables event driven interaction between main loop and psc consumer thread.
 * At this point it is used only in MemQ consumer case to inform memq consumer that the main loop encountered
 * the end of a batch while traversing messages from iterator.
 *
 * This is an evolving framework.
 */

public class PscEvent {

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
