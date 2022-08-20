package com.example;

import com.example.serde.GsonDeserializer;
import com.example.serde.GsonSerializer;
import com.example.util.TestDefaults;
import com.example.util.Transformers;
import com.google.common.collect.Lists;
import com.google.gson.annotations.JsonAdapter;
import marcono1234.gson.recordadapter.RecordTypeAdapterFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.commons.lang3.RandomStringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.function.BiPredicate;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimestampAwareStoreCleanerTest {

    @JsonAdapter(RecordTypeAdapterFactory.class)
    record MyRecord(
            String name,
            String description,
            Long from,
            Long to) {
    }

    private static final Serde<Integer> intSerde = Serdes.Integer();
    private static final Serializer<MyRecord> myRecordSerializer = new GsonSerializer<>();
    private static final Deserializer<MyRecord> myRecordDeserializer = new GsonDeserializer<>(MyRecord.class);
    private static final Serde<MyRecord> myRecordSerde = new WrapperSerde<>(myRecordSerializer, myRecordDeserializer);

    private static final String storeName = "%s_store".formatted(TimestampAwareStoreCleanerTest.class.getSimpleName());
    private static final String inputTopicName = "%s_inputTopic".formatted(TimestampAwareStoreCleanerTest.class.getSimpleName());
    private static final String outputTopicName = "%s_outputTopic".formatted(TimestampAwareStoreCleanerTest.class.getSimpleName());

    private static final Long now = System.currentTimeMillis();
    private static final Duration punctuationInterval = Duration.ofSeconds(1);

    private static final BiPredicate<MyRecord, Long> deleteIfTrue = (r, currentTimestamp) -> {
        var now = Instant.ofEpochMilli(currentTimestamp);
        var from = Instant.ofEpochMilli(r.from);
        var to = Instant.ofEpochMilli(r.to);
        // System.out.println("comparing range %d - %d to %d".formatted(from.toEpochMilli(), to.toEpochMilli(), now.toEpochMilli()));
        return now.isAfter(to) || now.isBefore(from);
    };

    private static final KeyValue<Integer, MyRecord> notYet = new KeyValue<>(
            1,
            new MyRecord(
                    RandomStringUtils.randomAlphanumeric(3),
                    RandomStringUtils.randomAlphanumeric(5),
                    now + 10000L,
                    now + 100000L
            )
    );
    private static final KeyValue<Integer, MyRecord> validNow = new KeyValue<>(
            2,
            new MyRecord(
                    RandomStringUtils.randomAlphanumeric(3),
                    RandomStringUtils.randomAlphanumeric(5),
                    now - 10000L,
                    now + 10000L
            )
    );
    private static final KeyValue<Integer, MyRecord> notAnymore = new KeyValue<>(
            3,
            new MyRecord(
                    RandomStringUtils.randomAlphanumeric(3),
                    RandomStringUtils.randomAlphanumeric(5),
                    now - 20000L,
                    now - 10000L
            )
    );

    private static final List<KeyValue<Integer, MyRecord>> data = List.of(notYet, validNow, notAnymore);

    private static final  Properties props = new Properties();

    private static final StreamsBuilder builder = new StreamsBuilder();

    @BeforeAll
    static void beforeAll(){
        props.putAll(TestDefaults.propMap);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, TimestampAwareStoreCleanerTest.class.getSimpleName());
    }

    @Test
    public void mustRetainOnlyCurrentlyValidEntriesInStore() {

        TimestampAwareStoreCleaner<Integer, MyRecord> cleaner = new TimestampAwareStoreCleaner<>(punctuationInterval, deleteIfTrue, storeName);

        Topology topology = createTopology(cleaner, builder, inputTopicName, outputTopicName, storeName);
        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, props, Instant.ofEpochMilli(now));
        TestOutputTopic<Integer, MyRecord> outputTopic = prepTestData(topologyTestDriver, inputTopicName, outputTopicName, data);

        System.out.println("advancing time by 2 sec");
        Duration advance = Duration.ofSeconds(2);
        topologyTestDriver.advanceWallClockTime(advance);

        KeyValueStore<Integer, MyRecord> store = topologyTestDriver.getKeyValueStore(storeName);

        List<KeyValue<Integer, MyRecord>> storeContents = Lists.newArrayList(store.all());
        assertEquals(1, storeContents.size());
        assertEquals(validNow, storeContents.get(0));

        var stats = cleaner.getStats();
        assertEquals(2, stats.getEvictedTotal());
        assertEquals(3, stats.getProcessedTotal());
    }

    @Test
    public void mustAbortAndResumeIfProcessingTakesTooLong(){

        MockProcessorContext  context  = new MockProcessorContext();//[Int, MyRecord] = new MockProcessorContext()
        KeyValueStore<Integer, MyRecord> store =
        Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(storeName),
                        Serdes.Integer(),
                        myRecordSerde
                )
                .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                .build();
        store.init(context, store);
        context.register(store, null);
        //context.addStateStore(store)

        // no time allowed to punctuate
        Long maxPunctuateMs = 100L;
        TimestampAwareStoreCleaner<Integer, MyRecord> cleaner = new TimestampAwareStoreCleaner<>(punctuationInterval, deleteIfTrue, storeName, PunctuationType.WALL_CLOCK_TIME, maxPunctuateMs);
        cleaner.init(context);

        store.putAll(data);

        final MockProcessorContext.CapturedPunctuator capturedPunctuator = context.scheduledPunctuators().get(0);
        final Punctuator punctuator = capturedPunctuator.getPunctuator();
        long now = System.currentTimeMillis();
        // let the punctuator abort
        context.setCurrentSystemTimeMs(now + maxPunctuateMs + 1);
        punctuator.punctuate(now);

        List<KeyValue<Integer, MyRecord>> storeContents = Lists.newArrayList(store.all());
        assertEquals(3, storeContents.size());
        //assertEquals(validNow, storeContents.get(0));

        var stats = cleaner.getStats();
        assertEquals(0, stats.getEvictedTotal());
        assertEquals(0, stats.getProcessedTotal());
        assertEquals(0, stats.getSkippedTotal());
        assertEquals(1, stats.getAbortedTotal());

        // test punctuator resume on initial key and process all value
        // this value will not be evicted
        System.out.println("resume processing - resumeKey was deleted - we miss one iteration");

        store.delete(1);

        context.setCurrentSystemTimeMs(now + maxPunctuateMs);
        punctuator.punctuate(now);

        assertEquals(0, stats.getEvictedTotal());
        assertEquals(0, stats.getProcessedTotal());
        assertEquals(2, stats.getSkippedTotal());
        assertEquals(1, stats.getAbortedTotal());

        System.out.println("resume processing - purge completed");

        context.setCurrentSystemTimeMs(now + maxPunctuateMs);
        punctuator.punctuate(now);

        List<KeyValue<Integer, MyRecord>> storeContentsAfterResume = Lists.newArrayList(store.all());
        assertEquals(1, storeContentsAfterResume.size());
        //assertEquals(validNow, storeContents.get(0));

        assertEquals(1, stats.getEvictedTotal());
        assertEquals(2, stats.getProcessedTotal());
        assertEquals(2, stats.getSkippedTotal());
        assertEquals(1, stats.getAbortedTotal());
    }


    private Topology createTopology(
            TimestampAwareStoreCleaner<Integer, MyRecord> cleaner,
            StreamsBuilder builder,
            String inputTopic,
            String outputTopic,
            String storeName
    ) {

        StoreBuilder<KeyValueStore<Integer, MyRecord>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(storeName),
                        intSerde,
                        myRecordSerde
                );

        builder.addStateStore(keyValueStoreBuilder);

        KStream<Integer, MyRecord> input =
                builder.stream(inputTopic,
                        Consumed.with(intSerde, myRecordSerde)
                );

        TransformerSupplier<Integer, MyRecord, KeyValue<Integer, MyRecord>> storeTransformSupplier = () -> Transformers.storeTransformer(storeName);

        TransformerSupplier<Integer, MyRecord, KeyValue<Integer, MyRecord>> ttlTransformSupplier = () ->  cleaner;

        var stored = input.transform(storeTransformSupplier, storeName);
        KStream<Integer, MyRecord> transformed = stored.transform(ttlTransformSupplier, storeName);
        transformed.to(outputTopic, Produced.with(intSerde, myRecordSerde));
        return builder.build();
    }

    private TestOutputTopic<Integer, MyRecord> prepTestData(
            TopologyTestDriver driver,
            String inputTopicName,
            String outputTopicName,
            List<KeyValue<Integer, MyRecord>> data
    ) {
        TestInputTopic<Integer, MyRecord> testInputTopic = driver.createInputTopic(
                inputTopicName,
                intSerde.serializer(),
                myRecordSerde.serializer()
        );

        TestOutputTopic<Integer, MyRecord> outputTopic = driver.createOutputTopic(
                outputTopicName,
                intSerde.deserializer(),
                myRecordSerde.deserializer()
        );

        testInputTopic.pipeKeyValueList(data);
        return outputTopic;
    }
}
