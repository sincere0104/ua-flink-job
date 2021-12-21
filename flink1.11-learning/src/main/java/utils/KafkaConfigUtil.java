package utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @ClassName KafkaConfigUtil
 * @Description TODO
 * @Author zby
 * @Date 2021-12-03 10:48
 * @Version 1.0
 **/
public class KafkaConfigUtil {

    private static final int RETRIES_CONFIG = 3;
    public static String broker_list = "kafka-server-01:9092";

    /**
     *
     * @param groupId groupId
     * @return ConsumerProps
     */
    public static Properties buildConsumerProps(String groupId) {
        Properties consumerProps = new Properties();
        //broker_list
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker_list);
        //group id
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //设置broker 的等待时间 默认500ms
        //consumerProps.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "5000");
        //这个参数用来配置 Consumer 在一次拉取请求中拉取的最大消息数，默认值为500条
        //consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20000");
        //动态发现分区
        //consumerProps.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,Long.toString(TimeUnit.MINUTES.toMillis(1)));
        return consumerProps;
    }

    /**
     *
     * @return ProducerProps
     */
    public static Properties buildProducerProps() {
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker_list);
        producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(RETRIES_CONFIG));
        producerProps.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "300000");
        producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "3");
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "1");

        return producerProps;
    }

}
