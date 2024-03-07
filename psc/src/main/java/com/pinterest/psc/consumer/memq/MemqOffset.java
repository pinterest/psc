package com.pinterest.psc.consumer.memq;


/* PSC offsets for memq messages:
 * |-1 bit----|--------- 45 bit ---------|------18 bit ------|
 * | reserved |          batch           |      message      |
 * |          |          offset          |      offset       |
 * |----------|--------------------------|-------------------|
 *
 * e.g. 0xffff_e000_0000_1000L -> batch offset 262143, message offset 4096
 *
 * Note:
 * This has to be structured this way since PscConsumerThread will perform increments on the offsets,
 * which should be equivalent to adding to message offsets
 */
public class MemqOffset {
  private long batchOffset;
  private long messageOffset;

  private final static int BATCH_OFFSET_BIT_LENGTH = 45;
  private final static int MESSAGE_OFFSET_BIT_LENGTH = 64 - BATCH_OFFSET_BIT_LENGTH;
  private final static long BATCH_OFFSET_MASK = (1L << BATCH_OFFSET_BIT_LENGTH) - 1;
  private final static int MESSAGE_OFFSET_MASK = (1 << MESSAGE_OFFSET_BIT_LENGTH) - 1;

  public MemqOffset(long batchOffset, long messageOffset) {
    this.batchOffset = batchOffset;
    this.messageOffset = messageOffset;
  }

  public long getBatchOffset() {
    return batchOffset;
  }

  public void setBatchOffset(long batchOffset) {
    this.batchOffset = batchOffset;
  }

  public long getMessageOffset() {
    return messageOffset;
  }

  public void setMessageOffset(long messageOffset) {
    this.messageOffset = messageOffset;
  }

  public static MemqOffset convertPscOffsetToMemqOffset(long pscOffset) {
    long batchOffset =  (pscOffset >>> MESSAGE_OFFSET_BIT_LENGTH) & BATCH_OFFSET_MASK;
    int messageOffset = (int) (pscOffset & MESSAGE_OFFSET_MASK);
    return new MemqOffset(batchOffset, messageOffset);
  }

  public long toLong() {
    return messageOffset | (batchOffset << MESSAGE_OFFSET_BIT_LENGTH);
  }

  @Override
  public String toString() {
    return "MemqOffset{" +
        "batchOffset=" + batchOffset +
        ", messageOffset=" + messageOffset +
        '}';
  }

}