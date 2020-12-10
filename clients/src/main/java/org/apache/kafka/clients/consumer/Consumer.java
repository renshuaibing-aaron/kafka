package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @see KafkaConsumer
 * @see MockConsumer
 */
public interface Consumer<K, V> extends Closeable {

    /**
     * @see KafkaConsumer#assignment()
     */
    public Set<TopicPartition> assignment();

    /**
     * @see KafkaConsumer#subscription()
     */
    public Set<String> subscription();

    /**
     *订阅指定的topics 并且为消费者指定分区
     * @see KafkaConsumer#subscribe(Collection)
     */
    public void subscribe(Collection<String> topics);

    /**
     * @see KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)
     */
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

    /**
     * @see KafkaConsumer#assign(Collection)
     *用户手动指定topic并且指定分区 注意这个方法和subscribe互斥
     */
    public void assign(Collection<TopicPartition> partitions);

    /**
    * @see KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)
    */
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

    /**
     * @see KafkaConsumer#unsubscribe()
     */
    public void unsubscribe();

    /**
     * @see KafkaConsumer#poll(long)
     * 拉取消息
     */
    public ConsumerRecords<K, V> poll(long timeout);

    /**
     * @see KafkaConsumer#commitSync()
     */
    public void commitSync();

    /**
     * @see KafkaConsumer#commitSync(Map)
     */
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * @see KafkaConsumer#commitAsync()
     */
    public void commitAsync();

    /**
     * @see KafkaConsumer#commitAsync(OffsetCommitCallback)
     */
    public void commitAsync(OffsetCommitCallback callback);

    /**
     * @see KafkaConsumer#commitAsync(Map, OffsetCommitCallback)
     */
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

    /**
     * @see KafkaConsumer#seek(TopicPartition, long)
     * 指定消费者的消费其实位置
     */
    public void seek(TopicPartition partition, long offset);

    /**
     * @see KafkaConsumer#seekToBeginning(Collection)
     */
    public void seekToBeginning(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#seekToEnd(Collection)
     */
    public void seekToEnd(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#position(TopicPartition)
     */
    public long position(TopicPartition partition);

    /**
     * @see KafkaConsumer#committed(TopicPartition)
     */
    public OffsetAndMetadata committed(TopicPartition partition);

    /**
     * @see KafkaConsumer#metrics()
     */
    public Map<MetricName, ? extends Metric> metrics();

    /**
     * @see KafkaConsumer#partitionsFor(String)
     */
    public List<PartitionInfo> partitionsFor(String topic);

    /**
     * @see KafkaConsumer#listTopics()
     */
    public Map<String, List<PartitionInfo>> listTopics();

    /**
     * @see KafkaConsumer#paused()
     * 暂停消费
     */
    public Set<TopicPartition> paused();

    /**
     * @see KafkaConsumer#pause(Collection)
     */
    public void pause(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#resume(Collection)
     * 继续消费
     */
    public void resume(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#close()
     */
    public void close();

    /**
     * @see KafkaConsumer#wakeup()
     */
    public void wakeup();

}
