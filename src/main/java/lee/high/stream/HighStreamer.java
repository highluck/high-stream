package lee.high.stream;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class HighStreamer<K, V> {
    private final Properties properties;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final String topic;

    private HighStreamer(final Properties properties,
                         final String applicationId,
                         final long commitIntervalMs,
                         final Serde<K> keySerde,
                         final Serde<V> valueSerde, final String topic) {
        this.topic = topic;
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, String.valueOf(commitIntervalMs));
        this.properties = properties;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public static <K1, V1> HighStreamer<K1, V1> of(final Properties properties,
                                                   final String applicationId,
                                                   final long commitIntervalMs,
                                                   final Class<K1> keyClass,
                                                   final Class<V1> valueClass,
                                                   final String topic) {
        final Serde<K1> keySerde = new Serdes.WrapperSerde<>(
                new KafkaSerializer<>(),
                new KafkaDeserializer<>(keyClass));
        final Serde<V1> valueSerde = new Serdes.WrapperSerde<>(
                new KafkaSerializer<>(),
                new KafkaDeserializer<>(valueClass));
        return new HighStreamer<>(properties,
                                  applicationId,
                                  commitIntervalMs,
                                  keySerde,
                                  valueSerde,
                                  topic);
    }

    public KStream<K, V> stream() {
        final StreamsBuilder builder = new StreamsBuilder();
        return builder.stream(topic);
    }

    public void start(KStream<K, V> stream) {

    }
}
