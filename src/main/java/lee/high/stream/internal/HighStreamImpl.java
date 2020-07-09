package lee.high.stream.internal;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import lee.high.stream.HighStream;
import lee.high.stream.HighWindowStore;
import lee.high.stream.HighWindowSuppressed;
import lee.high.stream.model.KafkaStreamsOperation;
import lee.high.stream.HighMaterialized;

public final class HighStreamImpl<INK, INV, OUTK, OUTV> implements HighStream<INK, INV, OUTK, OUTV> {
    private final Properties properties;
    private final Serde<OUTK> outKeySerde;
    private final Serde<OUTV> outValueSerde;
    private final String topic;
    private final String applicationId;
    private KafkaStreams kafkaStreams;

    public HighStreamImpl(final Properties properties,
                          final Serde<OUTK> outKeySerde,
                          final Serde<OUTV> outValueSerde,
                          final String topic,
                          final String applicationId) {
        this.topic = topic;
        this.properties = properties;
        this.outKeySerde = outKeySerde;
        this.outValueSerde = outValueSerde;
        this.applicationId = applicationId;
    }

    @Override
    public KafkaStreamsOperation streams(final Consumer<KStream<INK, INV>> stream) {
        final UncaughtExceptionHandler handler = (thread, exception) -> {
            // here you should examine the throwable/exception and perform an appropriate action!
        };

        return streams(stream, handler);
    }

    @Override
    public KafkaStreamsOperation streams(final Consumer<KStream<INK, INV>> stream,
                                         final UncaughtExceptionHandler e) {
        final StreamsBuilder builder = new StreamsBuilder();
        stream.accept(builder.stream(topic));
        final Topology topology = builder.build();
        kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.setUncaughtExceptionHandler(e);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaStreams.close()));
        return KafkaStreamsOperation.of(kafkaStreams, topology);
    }



    @Override
    public String topic() {
        return topic;
    }

    @Override
    public HighWindowStore store() {
        return new HighWindowStoreImpl(topic, applicationId);
    }

    @Override
    public HighWindowSuppressed suppressed() {
        return new HighWindowSuppressedImpl(topic, applicationId);
    }

    @Override
    public HighMaterialized<OUTK, OUTV> materialized() {
        return new HighMaterializedImpl<>(outKeySerde, outValueSerde);
    }
}
