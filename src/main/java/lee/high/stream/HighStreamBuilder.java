package lee.high.stream;

import java.util.Properties;

import lee.high.stream.serializers.KafkaSerde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;

import lee.high.stream.internal.HighStreamImpl;
import lee.high.stream.model.KeyValueSerde;
import lee.high.stream.model.StreamProperty;

public final class HighStreamBuilder<INK, INV> {
    private final Properties properties;
    private final String topic;
    private final String applicationId;
    private static Class keyClass;
    private static Class valueClass;

    public HighStreamBuilder(final StreamProperty streamProperty,
                             final String applicationId,
                             final long commitIntervalMs,
                             final KeyValueSerde<INK, INV> inKeyValueSerde,
                             final String topic) {
        this.applicationId = applicationId;
        this.topic = topic;
        properties = streamProperty.toProperty();
        keyClass = inKeyValueSerde.keyClass();
        valueClass = inKeyValueSerde.valueClass();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, String.valueOf(commitIntervalMs));
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, KeySerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ValueSerde.class.getName());
    }

    public static class KeySerde<INK> extends KafkaSerde<INK> {
        public KeySerde() {
            super(keyClass);
        }
    }

    public static class ValueSerde<INV> extends KafkaSerde<INV> {
        public ValueSerde() {
            super(valueClass);
        }
    }

    public HighStreamBuilder<INK, INV> setDeserializationExceptionHandler(
            final Class<DeserializationExceptionHandler> deserializationExceptionHandler) {
        properties.setProperty(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                               deserializationExceptionHandler.getName());
        return this;
    }

    public HighStream<INK, INV> build() {
        return new HighStreamImpl<>(properties, topic, applicationId);
    }
}
