package io.confluent.examples.streams.noc;

import lombok.Data;

/**
 * Created by dpoldrugo on 9/8/16.
 */
@Data
public final class DeltaMessage {
  private int sequenceId;

  private int countDelta;

  public DeltaMessage() {}

  public DeltaMessage(int sequenceId, int countDelta) {
    this.sequenceId = sequenceId;
    this.countDelta = countDelta;
  }

  public int getSequenceId() {
    return sequenceId;
  }

  public void setSequenceId(int sequenceId) {
    this.sequenceId = sequenceId;
  }

  public int getCountDelta() {
    return countDelta;
  }

  public void setCountDelta(int countDelta) {
    this.countDelta = countDelta;
  }
}
