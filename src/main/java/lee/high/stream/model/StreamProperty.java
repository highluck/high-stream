package lee.high.stream.model;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

public final class StreamProperty {
    private final String bootstrapServers;
    private final String autoOffsetReset;
    private final String stateStore;
    private final int maxPollRecords;
    private final int threadCount;
    private int sessionTimeoutMs;

    private StreamProperty(final String bootstrapServers,
                           final String autoOffsetReset,
                           final String stateStore,
                           final int maxPollRecords,
                           final int threadCount) {
        this.bootstrapServers = bootstrapServers;
        this.autoOffsetReset = autoOffsetReset;
        this.maxPollRecords = maxPollRecords;
        this.stateStore = stateStore;
        this.threadCount = threadCount;
    }

    public static StreamProperty of(final String bootstrapServers,
                                    final String autoOffsetReset,
                                    final String stateStore,
                                    final int maxPollRecords,
                                    final int threadCount) {
        return new StreamProperty(bootstrapServers,
                                  autoOffsetReset,
                                  stateStore,
                                  maxPollRecords,
                                  threadCount);
    }

    public StreamProperty setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        return this;
    }

    public Properties toProperty() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, String.valueOf(threadCount));
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateStore);
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
        if (sessionTimeoutMs != 0) {
            props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMs));
        }
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "41943040");
        props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "41943040");
        props.setProperty(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, "60000");
        props.setProperty(StreamsConfig.RECONNECT_BACKOFF_MS_CONFIG, "60000");
        return props;
    }
}
