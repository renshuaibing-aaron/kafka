package kafka.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public Producer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9090"); //kafka 服务端的主机名和端口号
        props.put("client.id", "DemoProducer"); //客户端ID
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer"); //  序列化器
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");//序列化器
        producer = new KafkaProducer<>(props); //核心类
        this.topic = topic;
        this.isAsync = isAsync;
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (messageNo<=100) {
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();


            if (isAsync) {
                 //异步发送
                ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topic, messageNo, messageStr);
                // Send asynchronously
                Future<RecordMetadata> send = producer.send(producerRecord, new DemoCallBack(startTime, messageNo, messageStr));


            } else { // Send synchronously
                try {
                    //同步发送
                    ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topic, messageNo, messageStr);
                    Future<RecordMetadata> future = producer.send(producerRecord);
                    RecordMetadata recordMetadata = future.get();
                    System.out.println("发送消息: (" + messageNo + ", " + messageStr + ")");
                    System.out.println("recordMetadata"+recordMetadata);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.  todo生产者发送消息的元数据 如果发送异常 参数为null
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                "消息(" + key + ", " + message + ") 发送的partition是(" + metadata.partition() +
                    "), " +
                    "offset是(" + metadata.offset() + ") 在 " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
