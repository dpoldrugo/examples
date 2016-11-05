package io.confluent.examples.streams.noc;

import lombok.Data;

/**
 * Created by dpoldrugo on 9/9/16.
 */
@Data
public class JoinMessageCurrentAndPrevious {

    private JoinMessage current;
    private JoinMessage previous;

    public JoinMessageCurrentAndPrevious() {}

    public JoinMessageCurrentAndPrevious(JoinMessage current, JoinMessage previous) {
        this.current = current;
        this.previous = previous;
    }
}
