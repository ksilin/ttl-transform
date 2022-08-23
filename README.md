# ttl-transform

This repository demonstrates a implementation for the cleaning of [Kafka Stream state stores](https://kafka.apache.org/32/documentation/streams/architecture#streams_architecture_state).

Regular, unwindowed state stores retain all of their entries and accumulate these over time. If left unattended, the state may become bloated. 
Kafka Streams does not implement a mechanism for removing state entries based on a condition, including time to live.

### How do I use it? 

Create a BiPredicate, use it to create an instance of the transformer and attach it to your Topology, e.g. 

```java
BiPredicate<MyRecord, Long> deleteIfTrue = (r, currentTimestamp) -> {
      var now = Instant.ofEpochMilli(currentTimestamp);
      var from = Instant.ofEpochMilli(r.from);
      var to = Instant.ofEpochMilli(r.to);
      return now.isAfter(to) || now.isBefore(from);
      };
TimestampAwareStoreCleaner<Integer, MyRecord> cleaner = new TimestampAwareStoreCleaner<>(punctuationInterval, deleteIfTrue, storeName, PunctuationType.WALL_CLOCK_TIME, 1000L );
TransformerSupplier<Integer, MyRecord, KeyValue<Integer, MyRecord>> ttlTransformSupplier = () ->  cleaner;
...
KStream<Integer, MyRecord> transformed = stored.transform(ttlTransformSupplier, storeName);
```

### How it works

The cleanup is implemented as a [Transformer](https://kafka.apache.org/32/javadoc/org/apache/kafka/streams/kstream/Transformer.html), but it drops every record passed to it. 
The transformer is used as a vehicle to register a [Punctuator](https://kafka.apache.org/32/javadoc/org/apache/kafka/streams/processor/Punctuator.html)

The cleaning condition is based on the record value and the current time of the context and passed as a `BiPredicate<V, Long>`

#### what if the iteration takes a long time?

Punctuators run in the main processing loop and thus must take care, not to take too much time in order to prevent timeouts, especially in transactional / EOS use cases.

The punctuator can track time, pause and resume.

TTL Transformer can be configured with `maxPunctuateMs`, after which the processing is aborted for the current run. 
The punctuator keeps track of the last key it has seen. 
On the next iteration, it skips forward to that key to resume processing.

* what if the record with the key has been removed between runs? 

The punctuator will skip through all records and reset the `resumeKey`.
No records will be evicted during next iteration. 
The iteration after that will resume normal processing. 

* what happens if the order of iteration is different between runs? 

In the worst case, no records will be evicted during next iteration.
The iteration after that will resume normal processing.

### improvements

* extend the cleaning predicate to be more generic, e.g. including the record key and/or the context 
* load-test it
* property-test it

### collaboration

Any for of collaboration on this project is encouraged - PRs, comments, issues etc. 
Let me know how it can improve or do it yourself.  