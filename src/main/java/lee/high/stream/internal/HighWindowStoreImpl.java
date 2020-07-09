package lee.high.stream.internal;

import java.time.Duration;

import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import lee.high.stream.HighWindowStore;

public class HighWindowStoreImpl implements HighWindowStore {
    private final String topic;
    private final String applicationId;
    private String storeName;

    public HighWindowStoreImpl(final String topic,
                               final String applicationId) {
        this.topic = topic;
        this.applicationId = applicationId;
    }

    @Override
    public WindowBytesStoreSupplier inMemoryWindowStore(final Duration windowTime) {
        return inMemoryWindowStore(topic + "-store", windowTime);
    }

    @Override
    public WindowBytesStoreSupplier inMemoryWindowStore(final String storeName, final Duration windowTime) {
        this.storeName = storeName;
        return Stores.inMemoryWindowStore(storeName,
                                          windowTime,
                                          windowTime,
                                          false);
    }

    @Override
    public WindowBytesStoreSupplier windowStore(final Duration windowTime) {
        return windowStore(topic + "-store", windowTime);
    }

    @Override
    public WindowBytesStoreSupplier windowStore(final String storeName, final Duration windowTime) {
        this.storeName = storeName;
        return Stores.persistentWindowStore(storeName,
                                            windowTime,
                                            windowTime,
                                            false);
    }

    @Override
    public SessionBytesStoreSupplier inMemorySessionStore(final Duration windowTime) {
        return inMemorySessionStore(topic + "-store", windowTime);
    }

    @Override
    public SessionBytesStoreSupplier inMemorySessionStore(final String storeName, Duration windowTime) {
        this.storeName = storeName;
        return Stores.inMemorySessionStore(storeName, windowTime);
    }

    @Override
    public SessionBytesStoreSupplier sessionStore(final Duration windowTime) {
        return sessionStore(topic + "-store", windowTime);
    }

    @Override
    public SessionBytesStoreSupplier sessionStore(final String storeName, final Duration windowTime) {
        this.storeName = storeName;
        return Stores.persistentSessionStore(storeName, windowTime);
    }

    @Override
    public String changeLog() {
        return String.format("%s-%s-changelog", applicationId, storeName);
    }

    @Override
    public String repartition() {
        return String.format("%s-%s-repartition", applicationId, storeName);
    }
}
