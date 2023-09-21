package com.pinterest.psc.consumer;

import java.util.Collection;
import com.pinterest.psc.common.TopicUriPartition;

public interface ConsumerRebalanceListener {
  void onPartitionsRevoked(Collection<TopicUriPartition> partitions);
  void onPartitionsAssigned(Collection<TopicUriPartition> partitions);
}
