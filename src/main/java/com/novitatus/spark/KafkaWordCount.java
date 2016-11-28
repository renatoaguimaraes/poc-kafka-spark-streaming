package com.novitatus.spark;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class KafkaWordCount
{
    private static final String SPACE = " ";

    public static void main(String[] args) throws Exception
    {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "grupo-2");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test");

        SparkConf sparkConf = new SparkConf().setAppName("KafkWordCount").setMaster("local");

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(30));

        ConsumerStrategy<String, String> subscribe = ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams);

        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(), subscribe);

        JavaDStream<String> words = stream.flatMap(message -> Arrays.asList(message.value().split(SPACE)).iterator());

        JavaPairDStream<String, Integer> wordsMap = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));

        JavaPairDStream<String, Integer> wordsCount = wordsMap.reduceByKey((v1, v2) -> v1 + v2);

        wordsCount.print();

        jsc.start();
        jsc.awaitTermination();
    }

}
