package com.hrbb.compute;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @ClassName ProductUpdateUsers
 * @Description TODO
 * @Author zby
 * @Date 2021-11-19 15:14
 * @Version 1.0
 **/
public class ProductUpdateUsers {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //提交作业时指定
        //StateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://cdh-master-01:8020/user/flink/checkpoint", true);
        //env.setStateBackend(rocksDBStateBackend);

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "kafka-server-01:9092");
        properties.setProperty("group.id", "group-fact-track-product-update-users");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Source
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>("event-insert", new SimpleStringSchema(), properties);
        // 默认的配置
        flinkKafkaConsumer.setStartFromGroupOffsets();

        // Sink
        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>("fact-track-product-update-users", new SimpleStringSchema(), properties);

        SingleOutputStreamOperator<String> streamSource = env.addSource(flinkKafkaConsumer)
                .name("KafkaSource")
                .uid("myKafkaConsumer");

        //主要逻辑，保存状态，发往下游
        streamSource
                .flatMap(new MyFlatMapFunction())
                .filter(new MyFilterFunction())
                .keyBy(new MyKeySelector())
                .process(new UserAppVersionKeyedProcessFunction())
                .uid("myUserAppVersionState")
                .addSink(flinkKafkaProducer)
                .name("KafkaSink");

        env.execute("ProductUpdateUsers");

    }
}
