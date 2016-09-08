package io.confluent.examples.streams.noc;

import lombok.Data;

/**
 * Created by dpoldrugo on 9/8/16.
 */
@Data
public final class JoinMessage {

  public JoinMessage() {}

  public JoinMessage(int sequenceId, Integer statusId, int countDelta) {
    this.sequenceId = sequenceId;
    this.statusId = statusId;
    this.countDelta = countDelta;
  }

  private int sequenceId;
  private Integer statusId;
  private int countDelta;

  public int getSequenceId() {
    return sequenceId;
  }

  public void setSequenceId(int sequenceId) {
    this.sequenceId = sequenceId;
  }

  public Integer getStatusId() {
    return statusId;
  }

  public void setStatusId(Integer statusId) {
    this.statusId = statusId;
  }

  public int getCountDelta() {
    return countDelta;
  }

  public void setCountDelta(int countDelta) {
    this.countDelta = countDelta;
  }
}
