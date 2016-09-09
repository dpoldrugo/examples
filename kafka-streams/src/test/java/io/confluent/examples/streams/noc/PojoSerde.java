package io.confluent.examples.streams.noc;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Created by dpoldrugo on 9/8/16.
 */
public class PojoSerde {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static <T> Serializer<T> serializer(Class<T> clazz) {
        return new Serializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {

            }

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
            public void close() {

            }
        };
    }

    public static <T> Deserializer<T> deserializer(Class<T> clazz) {
        return new Deserializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {

            }

            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null)
                    return null;
                try {
                    return mapper.readValue(data, clazz);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public void close() {

            }
        };
    }

}
