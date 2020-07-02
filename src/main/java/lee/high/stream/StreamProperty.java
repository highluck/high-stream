package lee.high.stream;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

public class StreamProperty {
    private String bootstrapServers;
    private String applicationId;
    private String autoOffsetReset;
    private int sessionTimeoutMs;
    private int maxPollRecords;
    private String stateStore;
    private int threadCount;

    private StreamProperty(final String bootstrapServers,
                           final String applicationId,
                           final String autoOffsetReset,
                           final int sessionTimeoutMs,
                           final int maxPollRecords,
                           final String stateStore,
                           final int threadCount) {
        this.bootstrapServers = bootstrapServers;
        this.applicationId = applicationId;
        this.autoOffsetReset = autoOffsetReset;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.maxPollRecords = maxPollRecords;
        this.stateStore = stateStore;
        this.threadCount = threadCount;
    }

    public static StreamProperty of(final String bootstrapServers,
                                    final String groupId,
                                    final String autoOffsetReset,
                                    final int sessionTimeoutMs,
                                    final int maxPollRecords,
                                    final String stateStore,
                                    final int threadCount) {
        return new StreamProperty(bootstrapServers,
                                  groupId,
                                  autoOffsetReset,
                                  sessionTimeoutMs,
                                  maxPollRecords,
                                  stateStore,
                                  threadCount);
    }

    public Properties toProperty() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG, String.valueOf(threadCount));
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateStore);
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMs));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "41943040");
        props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "41943040");
        props.setProperty(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, "60000");
        props.setProperty(StreamsConfig.RECONNECT_BACKOFF_MS_CONFIG, "60000");
        return props;
    }
}
