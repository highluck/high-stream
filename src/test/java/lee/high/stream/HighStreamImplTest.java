package lee.high.stream;

import lee.high.stream.model.KafkaStreamsOperation;
import lee.high.stream.model.KeyValueSerde;
import lee.high.stream.model.StreamProperty;
import lee.high.stream.serializers.KafkaSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.junit.Test;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.junit.Assert.assertTrue;

public class HighStreamImplTest {
    private static final Pattern COMPILE = Pattern.compile("PLAINTEXT://", Pattern.LITERAL);
    private final HighStream<Long, TestModel, Long, TestModel> highStream;
    private final KafkaProducer<Long, TestModel> kafkaProducer;
    private final String topic = "test-top7";
//    @ClassRule
//    public static SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
//            .registerListener(new PlainListener().onPorts(9092))
//            .withBrokers(1);

    //latest
    public HighStreamImplTest() {
        final Properties properties = new Properties();
//        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, COMPILE.matcher(sharedKafkaTestResource.getKafkaConnectString())
//                .replaceAll(Matcher.quoteReplacement("")));
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ACKS_CONFIG, "1");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, KafkaSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, KafkaSerializer.class.getName());
        kafkaProducer = new KafkaProducer<>(properties);

        final StreamProperty streamProperty = StreamProperty.of("localhost:9092",
                "latest",
                "/Users/high/high-stream",
                10,
                1);

        highStream = HighStreamBuilder.of(streamProperty,
                "test6",
                2000,
                KeyValueSerde.of(Long.class, TestModel.class),
                KeyValueSerde.of(Long.class, TestModel.class),
                topic)
                .build();
    }

    @Test
    public void 테스트() {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        final HighWindowStore store = highStream.store();
        final HighWindowSuppressed suppressed = highStream.suppressed();

        final Consumer<KStream<Long, TestModel>> streamKStream =
                stream -> stream
//                        .peek((k, v) -> System.out.println("xxxxxxxx " + v))
                        .groupByKey()
                        .windowedBy(SessionWindows.with(Duration.ofSeconds(5000))
                                .grace(Duration.ofSeconds(5000)))
                        .reduce((v1, v2) -> v2,
                                highStream.materialized()
                                        .as(store.inMemorySessionStore(Duration.ofSeconds(5000)))
                                        .withLoggingDisabled())

//                        .aggregate(TestModel::new,
//                                (key, value, aggregate) -> {
////                                    System.out.println("xxxxxx" + value);
//                                    return aggregate;
//                                },
//                                (key, aggOne, aggTwo) -> {
////                                    System.out.println("xxxxxx" + aggTwo.getValue());
//                                    return aggOne;
//                                },
//                                highStream.materialized()
//                                        .as(store.inMemorySessionStore(Duration.ofSeconds(5)))
//                                        .withLoggingDisabled())
//                        .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(5),
//                                Suppressed.BufferConfig.maxRecords(2)))
                        .toStream()
                        .foreach((k, v) -> {
                            System.out.println("result : " + v + " : " + k.key());
//                            countDownLatch.countDown();
                        });

        final KafkaStreamsOperation operation = highStream.streams(streamKStream);
        final String description = operation.topology().describe().toString();

        System.out.println("change : " + store.changeLog());
        assertTrue(description.contains(highStream.topic()));
//        assertTrue(description.contains("test-top-suppress-store"));
        operation.kafkaStreams().start();
//
//        send(new ProducerRecord<>(topic, 1L, new TestModel(1, "dasdsa")));
//        try {
//            Thread.sleep(1002);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        send(new ProducerRecord<>(topic, 2L, new TestModel(2, "dasdsa")));
//        send(new ProducerRecord<>(topic, 1L, new TestModel(3, "dasdsa")));
//        try {
//            Thread.sleep(1002);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        send(new ProducerRecord<>(topic, 2L, new TestModel(4, "dasdsa")));
//        send(new ProducerRecord<>(topic, 1L, new TestModel(5, "dasdsa")));
//        try {
//            Thread.sleep(1002);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        send(new ProducerRecord<>(topic, 2L, new TestModel(6, "dasdsa")));

        int count = 7;
        while (true) {
            send(new ProducerRecord<>(topic, 1L, new TestModel(count++, "dasdsa")));
            try {
                Thread.sleep(1002);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            send(new ProducerRecord<>(topic, 2L, new TestModel(count++, "dasdsa")));

//            try {
//                Thread.sleep(10000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            send(new ProducerRecord<>(topic, 2L, new TestModel(count++, "222222")));
            CompletableFuture.supplyAsync(() -> {
                send(new ProducerRecord<>(topic, 2L, new TestModel(99999, "222222")));
                send(new ProducerRecord<>(topic, 2L, new TestModel(19999, "222222")));
                send(new ProducerRecord<>(topic, 2L, new TestModel(59999, "dasdsa")));
                return true;
            });
            try {
                Thread.sleep(1002);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//            try {
//                countDownLatch.await();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
    }

    public void send(ProducerRecord<Long, TestModel> record) {
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.out.println("send fail!" + exception);
            }
        });
    }
}
