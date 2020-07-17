package lee.high.stream;

import java.util.Properties;

import lee.high.stream.serializers.KafkaSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;

import lee.high.stream.internal.HighStreamImpl;
import lee.high.stream.model.KeyValueSerde;
import lee.high.stream.model.StreamProperty;

public final class HighStreamBuilder<INK, INV, OUTK, OUTV> {
    private final Properties properties;
    private final Serde<OUTK> outKeySerde;
    private final Serde<OUTV> outValueSerde;
    private final String topic;
    private final String applicationId;
    private static Class keyClass;
    private static Class valueClass;

    private HighStreamBuilder(final StreamProperty streamProperty,
                              final String applicationId,
                              final long commitIntervalMs,
                              final KeyValueSerde<INK, INV> inKeyValueSerde,
                              final KeyValueSerde<OUTK, OUTV> outKeyValueSerde,
                              final String topic) {
        this.applicationId = applicationId;
        this.topic = topic;
        outKeySerde = outKeyValueSerde.keySerde();
        outValueSerde = outKeyValueSerde.valueSerde();
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

    public static <INK, INV, OUTK, OUTV> HighStreamBuilder<INK, INV, OUTK, OUTV> of(
            final StreamProperty streamProperty,
            final String applicationId,
            final long commitIntervalMs,
            final KeyValueSerde<INK, INV> inKeyValueSerde,
            final KeyValueSerde<OUTK, OUTV> outKeyValueSerde,
            final String topic) {
        return new HighStreamBuilder<>(streamProperty,
                                       applicationId,
                                       commitIntervalMs,
                                       inKeyValueSerde,
                                       outKeyValueSerde,
                                       topic);
    }

    public HighStreamBuilder<INK, INV, OUTK, OUTV> setDeserializationExceptionHandler(
            final Class<DeserializationExceptionHandler> deserializationExceptionHandler) {
        properties.setProperty(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                               deserializationExceptionHandler.getName());
        return this;
    }

    public HighStream<INK, INV> build() {
        return new HighStreamImpl<>(properties, outKeySerde, outValueSerde, topic, applicationId);
    }
}
