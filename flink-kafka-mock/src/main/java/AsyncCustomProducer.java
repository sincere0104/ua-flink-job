import com.hrbb.mock.EventInsertJson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @ClassName AsyncCustomProducer
 * @Description TODO 不带回调函数的API
 * @Author
 * @Date 2021-11-22 17:02
 * @Version 1.0
 **/
public class AsyncCustomProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();

        //kafka集群，broker-list
        props.put("bootstrap.servers", "130.1.10.163:9092");

        props.put("acks", "all");

        //重试次数
        props.put("retries", 1);

        //批次大小
        props.put("batch.size", 16384);

        //等待时间
        props.put("linger.ms", 1);

        //RecordAccumulator缓冲区大小
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 10000; i < 20000; i++) {
            producer.send(new ProducerRecord<String, String>("test-topic", Integer.toString(i), EventInsertJson.mock(i)));
        }

        producer.close();
    }
}
