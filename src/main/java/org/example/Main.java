package org.example;

import com.pipeline.facebook.FacebookFetcher;
import com.pipeline.spark.FacebookSparkConsumer;

public class Main {
    public static void main(String[] args) {
        // جلب البيانات من Facebook وإرسالها إلى Kafka في حلقة مستمرة
        Thread facebookThread = new Thread(() -> {
            while (true) {
                FacebookFetcher.fetchPosts();
                try {
                    Thread.sleep(30000); // poll كل 30 ثانية
                } catch (InterruptedException e) {
                    System.err.println("Facebook fetcher interrupted.");
                    break;
                }
            }
        });

        // تشغيل معالج Spark في خيط منفصل
        Thread sparkThread = new Thread(() -> {
            try {
                FacebookSparkConsumer.main(args);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        facebookThread.start();
        sparkThread.start();

        // انتظار انتهاء الـ threads (لو حصلت)
        try {
            facebookThread.join();
            sparkThread.join();
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted.");
        }
    }
}
