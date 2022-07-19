package com.example.util;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;

public class TestDefaults {

    public static final Map<String, Object> propMap = Map.of(StreamsConfig.STATE_DIR_CONFIG,
            TestUtils.tempDirectory().getAbsolutePath(),
            StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,0,
            StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10
            );

}
