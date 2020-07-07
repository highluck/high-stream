package lee.high.stream;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;

public final class HighStreamBuilder<INK, INV, OUTK, OUTV> {
    private final Properties properties;
    private final Serde<OUTK> outKeySerde;
    private final Serde<OUTV> outValueSerde;
    private final String topic;
    private final String applicationId;

    private HighStreamBuilder(final StreamProperty streamProperty,
                              final String applicationId,
                              final long commitIntervalMs,
                              final Serde<INK> keySerde,
                              final Serde<INV> valueSerde,
                              final Serde<OUTK> outKeySerde,
                              final Serde<OUTV> outValueSerde,
                              final String topic) {
        this.applicationId = applicationId;
        this.topic = topic;
        properties = streamProperty.toProperty();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, String.valueOf(commitIntervalMs));
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde.getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde.getClass().getName());
        this.outKeySerde = outKeySerde;
        this.outValueSerde = outValueSerde;
    }

    public static <INK1, INV1, OUTK1, OUTV1> HighStreamBuilder<INK1, INV1, OUTK1, OUTV1> of(final StreamProperty streamProperty,
                                                                final String applicationId,
                                                                final long commitIntervalMs,
                                                                final Class<INK1> keyClass,
                                                                final Class<INV1> valueClass,
                                                                final Class<OUTK1> outKeyClass,
                                                                final Class<OUTV1> outValueClass,
                                                                final String topic) {
        final Serde<INK1> keySerde = new KafkaSerde<>(keyClass);
        final Serde<INV1> valueSerde = new KafkaSerde<>(valueClass);
        final Serde<OUTK1> outKeySerde = new KafkaSerde<>(outKeyClass);
        final Serde<OUTV1> outValueSerde = new KafkaSerde<>(outValueClass);
        return new HighStreamBuilder<>(streamProperty,
                                       applicationId,
                                       commitIntervalMs,
                                       keySerde,
                                       valueSerde,
                                       outKeySerde,
                                       outValueSerde,
                                       topic);
    }

    public HighStreamBuilder<INK, INV, OUTK, OUTV> setDeserializationExceptionHandler(
            final Class<DeserializationExceptionHandler> deserializationExceptionHandler) {
        properties.setProperty(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                               deserializationExceptionHandler.getName());
        return this;
    }

    public HighStream<INK, INV, OUTK, OUTV> build() {
        return new HighStream<>(properties, outKeySerde, outValueSerde, topic, applicationId);
    }
}
