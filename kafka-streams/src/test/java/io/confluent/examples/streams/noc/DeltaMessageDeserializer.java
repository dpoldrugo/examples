package io.confluent.examples.streams.noc;

import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Created by dpoldrugo on 9/8/16.
 */
public class DeltaMessageDeserializer implements Deserializer<DeltaMessage> {

    private static final ObjectMapper mapper = new ObjectMapper();

    public DeltaMessage deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, DeltaMessage.class);
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
