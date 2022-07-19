package com.example.util;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class Transformers {

    public static <K, V> Transformer<K, V, KeyValue<K, V>> storeTransformer(String storeName) {

        Transformer<K, V, KeyValue<K, V>> transformer = new Transformer<>() {

            KeyValueStore<K, V> store;

            @Override
            public void init(ProcessorContext processorContext) {
                this.store = (KeyValueStore<K, V>) processorContext.getStateStore(storeName);
            }

            @Override
            public KeyValue<K, V> transform(K k, V v) {
                // logger.info(s"storing ${key}:${value} in $storeName ")
                store.put(k, v);
                return KeyValue.pair(k, v);
            }

            @Override
            public void close() {

            }
        };
        return transformer;
    }
}
