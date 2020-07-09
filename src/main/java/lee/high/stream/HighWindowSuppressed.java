package lee.high.stream;

import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;

public interface HighWindowSuppressed {
    Suppressed<Windowed> untilWindowCloses(final String suppressName);

    Suppressed<Windowed> untilWindowCloses();

    String changeLog();
}
