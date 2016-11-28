# poc-spark-streaming-kafka

```
Map<String, Object> kafkaParams = new HashMap<>();
kafkaParams.put("bootstrap.servers", "localhost:9092");
kafkaParams.put("key.deserializer", StringDeserializer.class);
kafkaParams.put("value.deserializer", StringDeserializer.class);
kafkaParams.put("group.id", "group");
kafkaParams.put("auto.offset.reset", "latest");
kafkaParams.put("enable.auto.commit", false);

SparkConf sparkConf = new SparkConf().setAppName("KafkWordCount").setMaster("local");

JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(30));

ConsumerStrategy<String, String> subscribe = ConsumerStrategies.<String, String> Subscribe(Arrays.asList("test"), kafkaParams);

final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(), subscribe);

JavaDStream<String> words = stream.flatMap(message -> Arrays.asList(message.value().split(SPACE)).iterator());

JavaPairDStream<String, Integer> wordsMap = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));

JavaPairDStream<String, Integer> wordsCount = wordsMap.reduceByKey((v1, v2) -> v1 + v2);

wordsCount.print();

jsc.start();
jsc.awaitTermination();
```