package com.pinterest.psc.consumer.listener;

import com.pinterest.psc.common.TopicUriPartition;
import com.pinterest.psc.consumer.ConsumerRebalanceListener;
import com.pinterest.psc.logging.PscLogger;

import java.util.Collection;

public class SimpleConsumerRebalanceListener implements ConsumerRebalanceListener {
  private int revoked = -1;
  private int assigned = -1;
  private static final PscLogger logger = PscLogger.getLogger(SimpleConsumerRebalanceListener.class);
  
  @Override
  public void onPartitionsRevoked(Collection<TopicUriPartition> partitions) {
    this.revoked = partitions.size();
    logger.info("Partitions revoked: " + revoked);
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicUriPartition> partitions) {
    this.assigned = partitions.size();
    logger.info("Partitions assigned: " + assigned);
  }
  
  public int getRevoked() { return this.revoked; }
  public int getAssigned() { return this.assigned; }
}
