package lee.high.stream;

import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;

public interface HighWindowSuppressed {
    Suppressed<Windowed> untilWindowCloses(final String suppressName);

    Suppressed<Windowed> untilWindowCloses();

    <K> Suppressed<K> untilTimeLimit(final Duration duration, final String suppressName);

    <K> Suppressed<K> untilTimeLimit(final Duration duration);

    String changeLog();
}
