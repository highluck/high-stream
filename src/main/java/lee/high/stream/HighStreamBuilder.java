package lee.high.stream;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;

import lee.high.stream.internal.HighStreamImpl;
import lee.high.stream.model.KeyValueSerde;
import lee.high.stream.model.KeyValueSerde.KeySerde;
import lee.high.stream.model.KeyValueSerde.ValueSerde;
import lee.high.stream.model.StreamProperty;

public final class HighStreamBuilder<INK, INV, OUTK, OUTV> {
    private final Properties properties;
    private final Serde<OUTK> outKeySerde;
    private final Serde<OUTV> outValueSerde;
    private final String topic;
    private final String applicationId;
    private static Class inKClass;
    private static Class inVClass;

    private HighStreamBuilder(final StreamProperty streamProperty,
                              final String applicationId,
                              final long commitIntervalMs,
                              final KeyValueSerde<INK, INV> inKeyValueSerde,
                              final KeyValueSerde<OUTK, OUTV> outKeyValueSerde,
                              final String topic) {
        this.applicationId = applicationId;
        this.topic = topic;
        inKClass = inKeyValueSerde.keyClass();
        inVClass = inKeyValueSerde.valueClass();
        outKeySerde = outKeyValueSerde.keySerde();
        outValueSerde = outKeyValueSerde.valueSerde();
        properties = streamProperty.toProperty();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, String.valueOf(commitIntervalMs));
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, KeySerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ValueSerde.class.getName());
    }

    public static <INK1, INV1, OUTK1, OUTV1> HighStreamBuilder<INK1, INV1, OUTK1, OUTV1> of(
            final StreamProperty streamProperty,
            final String applicationId,
            final long commitIntervalMs,
            final KeyValueSerde<INK1, INV1> inKeyValueSerde,
            final KeyValueSerde<OUTK1, OUTV1> outKeyValueSerde,
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

    public HighStream<INK, INV, OUTK, OUTV> build() {
        return new HighStreamImpl<>(properties, outKeySerde, outValueSerde, topic, applicationId);
    }
}
