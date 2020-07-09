package lee.high.stream.internal;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import lee.high.stream.HighMaterialized;

public final class HighMaterializedImpl<K, V> implements HighMaterialized<K, V> {
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    public HighMaterializedImpl(final Serde<K> keySerde,
                                final Serde<V> valueSerde) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public Materialized<K, V, WindowStore<Bytes, byte[]>> as(
            final WindowBytesStoreSupplier store) {
        return Materialized.<K, V>as(store)
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }

    @Override
    public Materialized<K, V, SessionStore<Bytes, byte[]>> as(
            final SessionBytesStoreSupplier store) {
        return Materialized.<K, V>as(store)
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }
}
