package com.example;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.BiPredicate;

public class TimestampAwareStoreCleaner<K, V> implements Transformer<K, V, KeyValue<K, V>> {

    private static final Logger log = LoggerFactory.getLogger(TimestampAwareStoreCleaner.class);

    public TimestampAwareStoreCleaner(Duration punctuateInterval,
                                      BiPredicate<V, Long> deleteIfTrue,
                                      String storeName) {
        this(punctuateInterval, deleteIfTrue, storeName, PunctuationType.WALL_CLOCK_TIME);
    }


    public TimestampAwareStoreCleaner(Duration punctuateInterval,
                                      BiPredicate<V, Long> deleteIfTrue,
                                      String storeName,
                                      PunctuationType punctuationType) {
        this.punctuateInterval = punctuateInterval;
        this.deleteIfTrue = deleteIfTrue;
        this.storeName = storeName;
        this.punctuationType = punctuationType;
    }

    private Duration punctuateInterval;
    private BiPredicate<V, Long> deleteIfTrue;
    private String storeName;
    private PunctuationType punctuationType;

    private ProcessorContext context;
    private KeyValueStore<K, V> store;
    private Cancellable cancellablePunctuator;


    @Override
    public void init(ProcessorContext context) {
        log.debug("initializing %s with context %s".formatted(getClass().getSimpleName(), context));
        this.context = context;
        store = this.context.getStateStore(storeName);
        log.debug("%s retrieved store %s".formatted(getClass().getSimpleName(), store));

        cancellablePunctuator = context.schedule(
                punctuateInterval,
                punctuationType,
                punctuator
        );
    }

    @Override
    public KeyValue<K, V> transform(K key, V value) {
        return null;
    }

    @Override
    public void close() {

    }

    public Cancellable getCancellablePunctuator() {
        return cancellablePunctuator;
    }

    private final Punctuator punctuator = now -> {
        log.debug("checking state store %s for records to remove at %d".formatted(storeName, now));
        try (final KeyValueIterator<K, V> all = store.all()) {
            while (all.hasNext()) {
                final KeyValue<K, V> record = all.next();
                boolean shouldDelete = deleteIfTrue.test(record.value, now);
                if (shouldDelete) {
                    log.debug("removing value for key %s from store %s".formatted(record.key, storeName));
                    store.delete(record.key);
                }
            }
        }
    };
}