package lee.high.stream.internal;

import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;

import lee.high.stream.HighWindowSuppressed;

import java.time.Duration;

public class HighWindowSuppressedImpl implements HighWindowSuppressed {
    private final String topic;
    private final String applicationId;
    private String suppressName;

    public HighWindowSuppressedImpl(final String topic,
                                    final String applicationId) {
        this.topic = topic;
        this.applicationId = applicationId;
    }

    @Override
    public Suppressed<Windowed> untilWindowCloses(final String suppressName) {
        this.suppressName = suppressName;
        return Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
                         .withName(suppressName);
    }

    @Override
    public Suppressed<Windowed> untilWindowCloses() {
        return untilWindowCloses(topic + "-suppress");
    }

    @Override
    public <K> Suppressed<K> untilTimeLimit(final Duration duration, final String suppressName) {
        this.suppressName = suppressName;
        return Suppressed.<K>untilTimeLimit(duration, Suppressed.BufferConfig.unbounded())
                .withName(suppressName);
    }

    @Override
    public <K> Suppressed<K> untilTimeLimit(final Duration duration) {
        return Suppressed.<K>untilTimeLimit(duration, Suppressed.BufferConfig.unbounded())
                .withName(suppressName);
    }

    @Override
    public String changeLog() {
        return String.format("%s-%s-store-changelog", applicationId, suppressName);
    }
}
