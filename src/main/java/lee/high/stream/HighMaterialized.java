package lee.high.stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.*;

public interface HighMaterialized<K, V> {
    Materialized<K, V, WindowStore<Bytes, byte[]>> as(final WindowBytesStoreSupplier store);

    Materialized<K, V, SessionStore<Bytes, byte[]>> as(final SessionBytesStoreSupplier store);

    <VR> Materialized<K, VR, KeyValueStore<Bytes, byte[]>> as(final KeyValueBytesStoreSupplier store,
                                                              final Serde<VR> valueSerde);
}