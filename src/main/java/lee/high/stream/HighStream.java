package lee.high.stream;

import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public final class HighStream<K, V> {
    private final Properties properties;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final String topic;
    private final String applicationId;
    private KafkaStreams kafkaStreams;
    private String storeName;
    private String suppressName;

    public HighStream(final Properties properties,
                      final Serde<K> keySerde,
                      final Serde<V> valueSerde,
                      final String topic,
                      final String applicationId) {
        this.topic = topic;
        this.properties = properties;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.applicationId = applicationId;
    }

    public KafkaStreams streams(final Consumer<KStream<K, V>> stream) {
        final UncaughtExceptionHandler handler = (thread, exception) -> {
            // here you should examine the throwable/exception and perform an appropriate action!
        };

        return streams(stream, handler);
    }

    public KafkaStreams streams(final Consumer<KStream<K, V>> stream,
                                final UncaughtExceptionHandler e) {
        final StreamsBuilder builder = new StreamsBuilder();
        stream.accept(builder.stream(topic));
        final Topology topology = builder.build();
        System.out.println("Topology info = " + topology.describe());
        kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.setUncaughtExceptionHandler(e);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaStreams.close()));
        return kafkaStreams;
    }

    public WindowBytesStoreSupplier inMemoryWindowStore(final Duration windowTime) {
        return inMemoryWindowStore(topic + "-store", windowTime);
    }

    public WindowBytesStoreSupplier inMemoryWindowStore(final String storeName, final Duration windowTime) {
        this.storeName = storeName;
        return Stores.inMemoryWindowStore(storeName,
                                          windowTime,
                                          windowTime,
                                          false);
    }

    public WindowBytesStoreSupplier windowStore(final Duration windowTime) {
        return windowStore(topic + "-store", windowTime);
    }

    public WindowBytesStoreSupplier windowStore(final String storeName, final Duration windowTime) {
        this.storeName = storeName;
        return Stores.persistentWindowStore(storeName,
                                            windowTime,
                                            windowTime,
                                            false);
    }

    public SessionBytesStoreSupplier inMemorySessionStore(final Duration windowTime) {
        return inMemorySessionStore(topic + "-store", windowTime);
    }

    public SessionBytesStoreSupplier inMemorySessionStore(final String storeName, Duration windowTime) {
        this.storeName = storeName;
        return Stores.inMemorySessionStore(storeName, windowTime);
    }

    public SessionBytesStoreSupplier sessionStore(final Duration windowTime) {
        return sessionStore(topic + "-store", windowTime);
    }

    public SessionBytesStoreSupplier sessionStore(final String storeName, final Duration windowTime) {
        this.storeName = storeName;
        return Stores.persistentSessionStore(storeName, windowTime);
    }

    public Materialized<K, V, WindowStore<Bytes, byte[]>> windowStoreMaterialized(
            final WindowBytesStoreSupplier store) {
        return Materialized.<K, V>as(store)
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }

    public Materialized<K, V, SessionStore<Bytes, byte[]>> sessionStoreMaterialized(
            final SessionBytesStoreSupplier store) {
        return Materialized.<K, V>as(store)
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }

    public Suppressed<Windowed> suppressed(final String suppressName) {
        this.suppressName = suppressName;
        return Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
                         .withName(suppressName);
    }

    public Suppressed<Windowed> suppressed() {
        return suppressed(topic + "-suppress");
    }

    public String topic() {
        return topic;
    }

    public String changeLog() {
        return String.format("%s-%s-changelog", applicationId, storeName);
    }

    public String repartition() {
        return String.format("%s-%s-repartition", applicationId, storeName);
    }

    public String suppress() {
        return String.format("%s-%s-store-changelog", applicationId, suppressName);
    }
}
