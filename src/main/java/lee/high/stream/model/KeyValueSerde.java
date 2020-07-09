package lee.high.stream.model;

import org.apache.kafka.common.serialization.Serde;

import lee.high.stream.serializers.KafkaSerde;

public final class KeyValueSerde<K, V> {
    private static Class keyClass;
    private static Class valueClass;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private KeyValueSerde(final Class<K> keyClass,
                          final Class<V> valueClass) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        keySerde = new KafkaSerde<>(keyClass);
        valueSerde = new KafkaSerde<>(valueClass);
    }

    public static <K1, V1> KeyValueSerde<K1, V1> of(final Class<K1> keyClass,
                                                    final Class<V1> valueClass) {
        return new KeyValueSerde<>(keyClass, valueClass);
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    public Class<K> keyClass() {
        return keyClass;
    }

    public Class<V> valueClass() {
        return valueClass;
    }

    public static class KeySerde extends KafkaSerde {
        public KeySerde() {
            super(keyClass);
        }
    }

    public static class ValueSerde extends KafkaSerde {
        public ValueSerde() {
            super(valueClass);
        }
    }
}
