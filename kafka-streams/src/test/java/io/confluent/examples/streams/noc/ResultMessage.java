package io.confluent.examples.streams.noc;

import lombok.Data;

/**
 * Created by dpoldrugo on 9/9/16.
 */
@Data
public class ResultMessage {

    private JoinMessageCurrentAndPrevious delta;
    private JoinMessageCurrentAndPrevious agg;

    public ResultMessage() {}

    public ResultMessage(JoinMessageCurrentAndPrevious delta, JoinMessageCurrentAndPrevious agg) {
        this.delta = delta;
        this.agg = agg;
    }
}
