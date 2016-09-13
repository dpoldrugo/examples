package io.confluent.examples.streams.pt;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Created by lmagdic on 13/09/16.
 */
public class PtDruidAggSerde extends AbstractSerde<PtDruidAgg> {

    @Override
    protected Class<PtDruidAgg> resolveClass() {
        return PtDruidAgg.class;
    }

/*
        implements Serializer<PtDruidAgg>, Deserializer<PtDruidAgg> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, PtDruidAgg data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public PtDruidAgg deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, PtDruidAgg.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }
*/
}
