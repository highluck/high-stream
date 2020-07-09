package lee.high.stream;

import java.time.Duration;

import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

public interface HighWindowStore {
    WindowBytesStoreSupplier inMemoryWindowStore(final Duration windowTime);

    WindowBytesStoreSupplier inMemoryWindowStore(final String storeName, final Duration windowTime);

    WindowBytesStoreSupplier windowStore(final Duration windowTime);

    WindowBytesStoreSupplier windowStore(final String storeName, final Duration windowTime);

    SessionBytesStoreSupplier inMemorySessionStore(final Duration windowTime);

    SessionBytesStoreSupplier inMemorySessionStore(final String storeName, Duration windowTime);

    SessionBytesStoreSupplier sessionStore(final Duration windowTime);

    SessionBytesStoreSupplier sessionStore(final String storeName, final Duration windowTime);

    String changeLog();

    String repartition();
}
