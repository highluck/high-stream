package lee.high.stream;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.junit.Test;

import lee.high.stream.model.KafkaStreamsOperation;
import lee.high.stream.model.KeyValueSerde;
import lee.high.stream.model.StreamProperty;
import lee.high.stream.serializers.KafkaSerializer;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HighStreamImplTest {
    private final HighStream<Long, TestModel, Long, TestModel> highStream;
    private final KafkaProducer<Long, TestModel> kafkaProducer;
    private final String topic = "test-topic";
//latest
    public HighStreamImplTest() {
        final Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ACKS_CONFIG, "1");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, KafkaSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, KafkaSerializer.class.getName());
        kafkaProducer = new KafkaProducer<>(properties);

        final StreamProperty streamProperty = StreamProperty.of("localhost:9092",
                                                                "test-stream",
                                                                "earliest",
                                                                "/Users/high/high-stream",
                                                                10,
                                                                1);

        highStream = HighStreamBuilder.of(streamProperty,
                                          "test",
                                          1000,
                                          KeyValueSerde.of(Long.class, TestModel.class),
                                          KeyValueSerde.of(Long.class, TestModel.class),
                                          topic)
                                      .build();
    }

//    @Test
    public void 테스트() {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        final HighWindowStore store = highStream.store();
        final HighWindowSuppressed suppressed = highStream.suppressed();

        final Consumer<KStream<Long, TestModel>> streamKStream =
                stream -> stream
                        .groupByKey()
                        .windowedBy(SessionWindows.with(Duration.ofSeconds(1)))
                        .aggregate(TestModel::new,
                                   (key, value, aggregate) -> {
                            System.out.println("xxxxxx" + aggregate);
                                        return value;
                                   },
                                   (key, aggOne, aggTwo) -> {
                                       System.out.println("xxxxxx" + aggOne.getValue());
                                            return aggOne;
                                   },
                                   highStream.materialized()
                                             .as(store.sessionStore(Duration.ofSeconds(1)))
                                             .withLoggingDisabled())
                        .suppress(suppressed.untilWindowCloses())
                        .toStream()
                        .peek((k, v) -> {
                            System.out.println("xxxxx" + v.getValue());
                            countDownLatch.countDown();
                        })
                        .to("test-out");

        final KafkaStreamsOperation operation = highStream.streams(streamKStream);
        final String description = operation.topology().describe().toString();

        System.out.println("change : " + store.changeLog());
        assertTrue(description.contains(highStream.topic()));
        assertTrue(description.contains("test-topic-suppress-store"));
        operation.kafkaStreams().start();

        final ProducerRecord<Long, TestModel> record = new ProducerRecord<>(topic, 1L, new TestModel(1, "dasdsa"));
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.out.println("send fail!" + exception);
            }
        });

        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.out.println("send fail!" + exception);
            }
        });

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
