package com.pipeline.facebook;

import com.google.gson.*;
import org.apache.kafka.clients.producer.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

public class FacebookFetcher {
    private static final String ACCESS_TOKEN = "EAAWwZAa4uUKkBOZB19kK8fQB83TZCj2vS3BBRQDFMrB1c1mYpscpuFNR6ctYU2Fmga0a9VZAzbkO46QbOtSPgjB7Q6RtaCEAIcB2RPe8jPZBK0Y2fieLgJmr9tPh6JHByZCg3w2SLuPkMS566w6H6O89rivR2IkZAkWiZCTGXqZCzWMmaaT1avIaaibhXVUBbUBDo77KHDvLV9aZAAnxjQWLtjcbkoKBFHGD4a";
    private static final String INITIAL_URL = "https://graph.facebook.com/v19.0/me/feed?fields=message,created_time&access_token=" + ACCESS_TOKEN;

    // الكلمات أو الهاشتاجات المهمة
    private static final List<String> keywords = Arrays.asList(  "masr", "Elsisi");

    public static void fetchPosts() {
        String url = INITIAL_URL;
        boolean hasNextPage = true;

        while (hasNextPage && url != null) {
            try {
                String jsonResponseStr = fetch(url);
                JsonObject jsonResponse = JsonParser.parseString(jsonResponseStr).getAsJsonObject();

                JsonArray posts = jsonResponse.getAsJsonArray("data");
                for (JsonElement postElement : posts) {
                    JsonObject post = postElement.getAsJsonObject();
                    if (post.has("message")) {
                        String message = post.get("message").getAsString();
                        String createdTime = post.get("created_time").getAsString();
                        String id = post.get("id").getAsString();

                        // فلترة على الكلمات أو الهاشتاجات
                        if (containsKeyword(message)) {
                            sendToKafka(message, createdTime, id);
                            saveToCSV(message, createdTime, id);
                        } else {
                            System.out.println("Filtered out: " + message);
                        }
                    }
                }

                // pagination
                if (jsonResponse.has("paging") && jsonResponse.getAsJsonObject("paging").has("next")) {
                    url = jsonResponse.getAsJsonObject("paging").get("next").getAsString();
                    System.out.println("Moving to next page: " + url);
                } else {
                    hasNextPage = false;
                }

            } catch (Exception e) {
                e.printStackTrace();
                hasNextPage = false;
            }
        }
    }

    private static boolean containsKeyword(String text) {
        String lowerText = text.toLowerCase();
        for (String keyword : keywords) {
            if (lowerText.contains(keyword.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    private static String fetch(String urlString) throws IOException {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        Scanner scanner = new Scanner(conn.getInputStream());
        StringBuilder sb = new StringBuilder();
        while (scanner.hasNext()) {
            sb.append(scanner.nextLine());
        }
        scanner.close();

        return sb.toString();
    }

    private static void sendToKafka(String message, String createdTime, String id) {
        String topic = "facebook-posts";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        JsonObject json = new JsonObject();
        json.addProperty("text", message);
        json.addProperty("timestamp", createdTime);
        json.addProperty("post_id", id);

        String jsonString = new Gson().toJson(json);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonString);
            producer.send(record);
            System.out.println("Sent to Kafka: " + jsonString);
        }
    }

    private static void saveToCSV(String message, String createdTime, String id) {
        String filePath = "filtered_posts.csv";
        File file = new File(filePath);
        boolean fileExists = file.exists();

        try (PrintWriter pw = new PrintWriter(new FileWriter(filePath, true))) {
            if (!fileExists) {
                pw.println("message,created_time,post_id");
            }
            pw.printf("\"%s\",\"%s\",\"%s\"%n",
                    message.replace("\"", "\"\""),
                    createdTime,
                    id);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
