package lee.high.stream;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.function.Consumer;

import org.apache.kafka.streams.kstream.KStream;

import lee.high.stream.model.KafkaStreamsOperation;

public interface HighStream<INK, INV, OUTK, OUTV> {
    KafkaStreamsOperation streams(final Consumer<KStream<INK, INV>> stream);

    KafkaStreamsOperation streams(final Consumer<KStream<INK, INV>> stream,
                                  final UncaughtExceptionHandler e);

    String topic();

    HighWindowStore store();

    HighWindowSuppressed suppressed();

    HighMaterialized<OUTK, OUTV> materialized();
}
