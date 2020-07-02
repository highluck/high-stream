package lee.high.stream;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaSerializer<T> implements Serializer<T> {
    private ObjectMapper objectMapper;

    public KafkaSerializer() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public void configure(Map<String, ?> settings, boolean isKey) { }

    @Override
    public byte[] serialize(String topic, T message) {
        if (null == message) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {

    }
}
