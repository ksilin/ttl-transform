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
        this(punctuateInterval, deleteIfTrue, storeName, PunctuationType.WALL_CLOCK_TIME, Long.MAX_VALUE);
    }

    public TimestampAwareStoreCleaner(Duration punctuateInterval,
                                      BiPredicate<V, Long> deleteIfTrue,
                                      String storeName,
                                      PunctuationType punctuationType,
                                      Long maxPunctuateMs) {
        this.punctuateInterval = punctuateInterval;
        this.deleteIfTrue = deleteIfTrue;
        this.storeName = storeName;
        this.punctuationType = punctuationType;
        this.maxPunctuateMs = maxPunctuateMs;
    }

    private final Duration punctuateInterval;
    private BiPredicate<V, Long> deleteIfTrue;
    private String storeName;
    private final PunctuationType punctuationType;

    private Long maxPunctuateMs;

    private K resumeKey = null;

    private ProcessorContext context;
    private KeyValueStore<K, V> store;
    private Cancellable cancellablePunctuator;

    private final StoreCleanerStats stats = new StoreCleanerStats();

    @Override
    public void init(ProcessorContext context) {
        log.debug("initializing {} with context {}.{}", getClass().getSimpleName(), context.applicationId(), context.taskId());
        this.context = context;
        store = this.context.getStateStore(storeName);
        log.debug("{} retrieved store {}", getClass().getSimpleName(), store);

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

    private final Punctuator punctuator = punctuateStartTime -> {
        log.debug("checking state store {} for records to remove at {}", storeName, punctuateStartTime);
        try (final KeyValueIterator<K, V> all = store.all()) {

            int processed = 0;
            int evicted = 0;
            int skipped = 0;
            boolean aborted = false;

            // rewind to resumeKey if required
            // if resume key was deleted in the meantime, we skip this iteration
            while (resumeKey != null && all.hasNext() && all.peekNextKey() != resumeKey) {
                log.trace("skipping record with key {}, trying to advance to resumeKey {}", all.peekNextKey(), resumeKey);
                all.next();
                skipped = skipped + 1;
            }
            resumeKey = null;

            while (all.hasNext()) {
                final KeyValue<K, V> record = all.next();

                long elapsedPunctuationTime = context.currentSystemTimeMs() - punctuateStartTime;
                log.trace("elapsed time: {}, maxTime: {}", elapsedPunctuationTime, maxPunctuateMs);
                if (elapsedPunctuationTime > this.maxPunctuateMs) {
                    resumeKey = record.key;
                    aborted = true;
                    stats.incrementAbortedTotal();
                    log.debug("elapsed {} ms for punctuation of store {}, which is more than the allowed {} ms. Aborting this iteration and memorizing key {} to resume with", elapsedPunctuationTime, storeName, maxPunctuateMs, resumeKey);
                    break;
                }

                boolean shouldDelete = deleteIfTrue.test(record.value, punctuateStartTime);
                if (shouldDelete) {
                    log.debug("removing value for key {} from store {}", record.key, storeName);
                    store.delete(record.key);
                    evicted = evicted + 1;
                }
                processed = processed + 1;
            }
            // always reset the resumeKey if fully iterated
            if (!aborted) resumeKey = null;
            stats.incrementEvictedTotal(evicted);
            stats.incrementProcessedTotal(processed);
            stats.incrementSkippedTotal(skipped);
        }
    };

    public StoreCleanerStats getStats() {
        return stats;
    }
}