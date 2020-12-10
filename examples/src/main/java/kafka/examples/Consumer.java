package kafka.examples;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class Consumer extends ShutdownableThread {
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;

    public Consumer(String topic) {
        super("KafkaConsumerExample", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9090");  //broker地址
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer"); //所属consumergroup的ID
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); //自动提交offset
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); //自动提交offset的时间间隔
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");//?

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer"); //key使用的Deserializer
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");//value 使用的Deserializer

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    @Override
    public void doWork() {
        //订阅topic
        consumer.subscribe(Collections.singletonList(this.topic));
        //从服务端拉取消息 每次poll()可以拉取多个消息
        //为了保证消息 不丢失 在每次poll前尽量保证消息消息完毕
        ConsumerRecords<Integer, String> records = consumer.poll(5);


        //消费消息 注意这个可以把消息的key value 和offset打印出来
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println("接收到消息: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        }
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }
}
