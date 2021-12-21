import com.hrbb.mock.EventInsertJson;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.lang.Thread.sleep;

/**
 * @ClassName AsyncCustomProducerWithCall
 * @Description TODO 带回调函数的API
 * 回调函数会在producer收到ack时调用，为异步调用，该方法有两个参数，分别是RecordMetadata和Exception，如果Exception为null，说明消息发送成功，如果Exception不为null，说明消息发送失败。
 * @Author zby
 * @Date 2021-11-22 16:56
 * @Version 1.0
 **/
public class AsyncCustomProducerWithCall {
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

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i < 10000; i++) {

            if (i%200 == 0){
                sleep(1000);
            }

            producer.send(new ProducerRecord<String, String>("test-topic", Integer.toString(i), EventInsertJson.mock(i)), new Callback() {
                //回调函数，该方法会在Producer收到ack时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("success->" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }
        producer.close();
    }
}

