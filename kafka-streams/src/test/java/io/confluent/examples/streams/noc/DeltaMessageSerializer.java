package io.confluent.examples.streams.noc;

import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Created by dpoldrugo on 9/8/16.
 */
public class DeltaMessageSerializer implements Serializer<DeltaMessage> {

    private static final ObjectMapper mapper = new ObjectMapper();

    public byte[] serialize(String topic, DeltaMessage data) {
        try {
            return mapper.writeValueAsBytes(data);
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
}
