package lee.high.stream;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public interface HighMaterialized<K, V> {
    Materialized<K, V, WindowStore<Bytes, byte[]>> as(final WindowBytesStoreSupplier store);

    Materialized<K, V, SessionStore<Bytes, byte[]>> as(final SessionBytesStoreSupplier store);
}
