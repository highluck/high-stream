package lee.high.stream.serializers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaSerde<T> implements Serde<T> {
    private KafkaDeserializer<T> kafkaDeserializer;
    private KafkaSerializer<T> kafkaSerializer;

    public KafkaSerde(Class<T> tClass) {
        kafkaDeserializer = new KafkaDeserializer<>(tClass);
        kafkaSerializer = new KafkaSerializer<>();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        kafkaSerializer.configure(configs, isKey);
        kafkaDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        kafkaDeserializer.close();
        kafkaSerializer.close();
    }

    @Override
    public Serializer<T> serializer() {
        return kafkaSerializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return kafkaDeserializer;
    }
}
