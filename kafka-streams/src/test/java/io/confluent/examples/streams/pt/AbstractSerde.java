package io.confluent.examples.streams.pt;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Created by lmagdic on 13/09/16.
 */
public abstract class AbstractSerde<T> implements Serializer<T>, Deserializer<T> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, resolveClass());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    protected abstract Class<T> resolveClass();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }


}
