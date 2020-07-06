package lee.high.stream;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.state.SessionStore;
import org.junit.Test;

public class HighStreamTest {
    private final HighStream<Long, TestModel> highStream;

    public HighStreamTest() {
        final StreamProperty streamProperty = StreamProperty.of("localhost:9092",
                                                                "test-stream",
                                                                "latest",
                                                                10,
                                                                "/Users/high/high-stream",
                                                                1);

        highStream = HighStreamBuilder.of(streamProperty,
                                          "test",
                                          1000,
                                          Long.class,
                                          TestModel.class,
                                          "test-topic")
                                      .build();
    }

    @Test
    public void 테스트() {
        final Consumer<KStream<Long, TestModel>> streamKStream =
                stream -> stream.groupBy((k, v) -> k)
                            .windowedBy(SessionWindows.with(Duration.ofSeconds(20)))
                            .aggregate(TestModel::new,
                                 (key, value, aggregate) -> value,
                                 (key, aggOne, aggTwo) -> aggTwo,
                                 highStream.sessionStoreMaterialized(
                                         highStream.sessionStore(Duration.ofSeconds(20))))
                            .suppress(highStream.suppressed())
                            .toStream()
                            .to("test-out");

        highStream.streams(streamKStream).start();
    }

}
