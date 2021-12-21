package connect;

import mockdata.GoodsGenerator;
import mockdata.Order;
import mockdata.OrderGenerator;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

/**
 * @ClassName Test
 * @Description TODO
 * @Author zby
 * @Date 2021-12-03 10:10
 * @Version 1.0
 **/
public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<Order> orderDataStreamSource = executionEnvironment.addSource(new OrderGenerator());
//        orderDataStreamSource.filter(Objects::nonNull).print();

        executionEnvironment.addSource(new GoodsGenerator()).print();
        executionEnvironment.execute("aaa");
    }
}
