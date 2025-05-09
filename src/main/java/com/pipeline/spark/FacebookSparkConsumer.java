package com.pipeline.spark;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import scala.Tuple2;

import java.util.*;

public class FacebookSparkConsumer {
    public static void main(String[] args) throws InterruptedException {
        // تقليل السجلات
        Logger.getLogger("org").setLevel(Level.WARN);

        // إعداد Spark
        SparkConf conf = new SparkConf()
                .setAppName("FacebookSparkConsumer")
                .setMaster("local[*]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // إعدادات Kafka
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "facebook-consumer-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList("facebook-posts");

        // إنشاء DStream
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> messages = stream.map(ConsumerRecord::value);

        messages.foreachRDD(rdd -> {
            System.out.println("\n=== New Batch ===");
            System.out.println("Total messages received: " + rdd.count());

            JavaPairRDD<String, Integer> wordCounts = rdd
                    .filter(message -> {
                        try {
                            JsonParser.parseString(message);
                            return true;
                        } catch (Exception e) {
                            System.err.println("Invalid JSON: " + message);
                            return false;
                        }
                    })
                    .flatMapToPair(message -> {
                        List<Tuple2<String, Integer>> words = new ArrayList<>();
                        try {
                            JsonObject json = JsonParser.parseString(message).getAsJsonObject();

                            if (json.has("text")) {
                                String text = json.get("text").getAsString();
                                String[] wordsArray = text.split("\\s+");

                                for (String word : wordsArray) {
                                    String cleanedWord = word.toLowerCase().replaceAll("[^a-zA-Z#]", "");
                                    if (!cleanedWord.isEmpty()) {
                                        words.add(new Tuple2<>(cleanedWord, 1));
                                    }
                                }
                            }

                        } catch (Exception e) {
                            System.err.println("Error processing message: " + e.getMessage());
                        }
                        return words.iterator();
                    })
                    .reduceByKey(Integer::sum);

            // ترتيب و عرض النتائج
            List<Tuple2<String, Integer>> sortedCounts = wordCounts
                    .mapToPair(Tuple2::swap)
                    .sortByKey(false)
                    .mapToPair(Tuple2::swap)
                    .collect();

            System.out.println("\nWord Count Results:");
            for (Tuple2<String, Integer> tuple : sortedCounts) {
                System.out.println(tuple._1 + ": " + tuple._2);
            }

            System.out.println("\nHashtags Count:");
            sortedCounts.stream()
                    .filter(tuple -> tuple._1.startsWith("#"))
                    .forEach(tuple -> System.out.println(tuple._1 + ": " + tuple._2));
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
