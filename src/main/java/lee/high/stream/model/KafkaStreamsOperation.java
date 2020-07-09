package lee.high.stream.model;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaStreamsOperation {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsOperation.class);

    private final KafkaStreams kafkaStreams;
    private final Topology topology;

    private KafkaStreamsOperation(final KafkaStreams kafkaStreams, final Topology topology) {
        this.kafkaStreams = kafkaStreams;
        this.topology = topology;
        log.info("Topology info = {}", topology.describe());
    }

    public static KafkaStreamsOperation of(final KafkaStreams kafkaStreams, final Topology topology) {
        return new KafkaStreamsOperation(kafkaStreams, topology);
    }

    public KafkaStreams kafkaStreams() {
        return kafkaStreams;
    }

    public Topology topology() {
        return topology;
    }
}
