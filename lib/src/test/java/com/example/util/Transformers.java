package com.example.util;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.text.SimpleDateFormat;

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

    private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

    public static  <K, V> ValueTransformerWithKeySupplier<K, V, V> timeLoggingValueTransformerSupplier(String name ){
        return () -> timeLoggingValueTransformer(name);
    }

    public static <K, V> ValueTransformerWithKey<K, V, V> timeLoggingValueTransformer(String name ) {
        return createCtxAwareValueTransformer((k, v, ctx) -> {
            System.out.println(name + " : processing" + k + " : " + v);
            System.out.println("task: "+ ctx.taskId());
            System.out.println("topic: "+ ctx.topic());
            System.out.println("partition: "+ ctx.partition());
            System.out.println("offset: "+ ctx.offset());
            System.out.println("wallclock: " + df.format(ctx.currentSystemTimeMs()));
            System.out.println("stream:  " +  df.format(ctx.currentStreamTimeMs()));
            System.out.println("record ts: " + df.format(ctx.timestamp()));
            return v;
        });
    }


    public static <K, V, R> ValueTransformerWithKey<K, V, R> createCtxAwareValueTransformer(TriFunction<K, V, ProcessorContext, R> fun) {
        return new ValueTransformerWithKey<K, V, R >() {

            ProcessorContext ctx;

            @Override
            public void init(ProcessorContext processorContext) {
                ctx = processorContext;
            }

            @Override
            public R transform(K k, V v) {
                return fun.apply(k, v, ctx);
            }

            @Override
            public void close() {
            }

        };
    }
}
