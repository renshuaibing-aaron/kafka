package kafka.examples;

import java.util.concurrent.CountDownLatch;

public class KafkaConsumerProducerDemo {
    public static void main(String[] args) throws InterruptedException {
        Consumer consumerThread = new Consumer(KafkaProperties.TOPIC_ELK);
        consumerThread.start();


        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        Producer producerThread = new Producer(KafkaProperties.TOPIC_ELK, false);
        producerThread.start();

        new CountDownLatch(1).wait();


    }
}
