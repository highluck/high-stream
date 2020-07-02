package lee.high.stream;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper;
    Class<T> cls;

    public KafkaDeserializer() {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, true);
    }

    public KafkaDeserializer(Class<T> cls) {
        this();
        this.cls = cls;
    }

    public static Map<String, String> nonDefaultSettings(ObjectMapper objectMapper) {
        return JacksonDeserializerConfig.nonDefaultSettings(objectMapper);
    }

    @Override
    public void configure(Map<String, ?> settings, boolean isKey) {
        if (cls == null) {
            final JacksonDeserializerConfig config = new JacksonDeserializerConfig(settings);
            config.configure(objectMapper);
            cls = config.outputClass;
        }
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (null == bytes) {
            return null;
        }

        try {
            return objectMapper.readValue(bytes, cls);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {
    }
}
