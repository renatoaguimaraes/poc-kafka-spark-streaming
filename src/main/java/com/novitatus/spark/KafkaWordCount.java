package com.novitatus.spark;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
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

    private KafkaWordCount()
    {
    }

    public static void main(String[] args) throws Exception
    {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test");

        SparkConf sparkConf = new SparkConf().setAppName("KafkWordCount").setMaster("local");

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(30));

        ConsumerStrategy<String, String> subscribe = ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams);

        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(), subscribe);

        JavaDStream<String> words = stream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>()
        {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> t) throws Exception
            {
                return Arrays.asList(t.value().split(SPACE)).iterator();
            }
        });

        JavaPairDStream<String, Integer> wordsMap = words.mapToPair(new PairFunction<String, String, Integer>()
        {
            @Override
            public Tuple2<String, Integer> call(String t) throws Exception
            {
                return new Tuple2<String, Integer>(t, 1);
            }
        });

        JavaPairDStream<String, Integer> wordsCount = wordsMap.reduceByKey(new Function2<Integer, Integer, Integer>()
        {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception
            {
                return v1 + v2;
            }
        });

        wordsCount.print(100);

        jsc.start();
        jsc.awaitTermination();
    }
}
