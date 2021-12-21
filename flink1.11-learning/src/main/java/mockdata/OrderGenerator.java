package mockdata;

import org.apache.flink.shaded.curator4.org.apache.curator.RetrySleeper;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * @ClassName OrderGenerator
 * @Description TODO
 * @Author zby
 * @Date 2021-12-03 09:59
 * @Version 1.0
 **/
public class OrderGenerator extends RichParallelSourceFunction<Order> {
    private static final Random RANDOM = new Random();
    private static long orderId = 0;

    @Override
    public void run(SourceContext<Order> sourceContext) throws Exception {
        while (true) {

            int cityId = RANDOM.nextInt(10);
            if (cityId == 0) {
                sourceContext.collect(null);
            } else {

                String orderId = "orderId:" + OrderGenerator.orderId++;
                int userIdMax = 10_000_000;
                String userId = Integer.toString(RANDOM.nextInt(userIdMax));
                int goodsId = RANDOM.nextInt(10);
                int price = RANDOM.nextInt(10000);
                Order order = new Order(System.currentTimeMillis(),
                        orderId, userId, goodsId, price, cityId);
                sourceContext.collect(order);
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}
