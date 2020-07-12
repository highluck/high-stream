package lee.high.stream.internal;

import lee.high.stream.HighMaterialized;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.*;

public final class HighMaterializedImpl<K, V> implements HighMaterialized<K, V> {
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    public HighMaterializedImpl(final Serde<K> keySerde,
                                final Serde<V> valueSerde) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public Materialized<K, V, WindowStore<Bytes, byte[]>> as(final WindowBytesStoreSupplier store) {
        return Materialized.<K, V>as(store)
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }

    @Override
    public Materialized<K, V, SessionStore<Bytes, byte[]>> as(final SessionBytesStoreSupplier store) {
        return Materialized.<K, V>as(store)
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }

    @Override
    public <VR> Materialized<K, VR, KeyValueStore<Bytes, byte[]>> as(final KeyValueBytesStoreSupplier store,
                                                                     final Serde<VR> valueSerde) {
        return Materialized.<K, VR>as(store)
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }
}
