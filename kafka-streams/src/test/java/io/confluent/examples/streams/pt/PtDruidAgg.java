package io.confluent.examples.streams.pt;

import lombok.Data;

/**
 * Created by lmagdic on 13/09/16.
 */
@Data
public final class PtDruidAgg {

    private int statusId;
    private int count;

    public PtDruidAgg() {}

    public PtDruidAgg(int statusId, int count) {
        this.statusId = statusId;
        this.count = count;
    }
}
