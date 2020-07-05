package lee.high.stream;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;

public final class HighStreamBuilder<K, V> {
    private final Properties properties;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final String topic;
    private final String applicationId;

    private HighStreamBuilder(final Properties properties,
                              final String applicationId,
                              final long commitIntervalMs,
                              final Serde<K> keySerde,
                              final Serde<V> valueSerde,
                              final String topic) {
        this.applicationId = applicationId;
        this.topic = topic;
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, String.valueOf(commitIntervalMs));
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde.getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde.getClass().getName());
        this.properties = properties;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public static <K1, V1> HighStreamBuilder<K1, V1> of(final Properties properties,
                                                        final String applicationId,
                                                        final long commitIntervalMs,
                                                        final Class<K1> keyClass,
                                                        final Class<V1> valueClass,
                                                        final String topic) {
        final Serde<K1> keySerde = new KafkaSerde<>(keyClass);
        final Serde<V1> valueSerde = new KafkaSerde<>(valueClass);
        return new HighStreamBuilder<>(properties,
                                       applicationId,
                                       commitIntervalMs,
                                       keySerde,
                                       valueSerde,
                                       topic);
    }

    public HighStreamBuilder<K, V> setDeserializationExceptionHandler(
            final Class<DeserializationExceptionHandler> deserializationExceptionHandler) {
        properties.setProperty(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                               deserializationExceptionHandler.getName());
        return this;
    }

    public HighStream<K, V> build() {
        return new HighStream<>(properties, keySerde, valueSerde, topic, applicationId);
    }
}
