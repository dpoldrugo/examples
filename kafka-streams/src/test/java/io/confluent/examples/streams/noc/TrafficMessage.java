package io.confluent.examples.streams.noc;

import lombok.Data;

/**
 * Created by dpoldrugo on 9/8/16.
 */
@Data
public final class TrafficMessage {

  private int sequenceId;

  private Integer statusId;

  public TrafficMessage() {}

  public TrafficMessage(int sequenceId, Integer statusId) {
    this.sequenceId = sequenceId;
    this.statusId = statusId;
  }

}
