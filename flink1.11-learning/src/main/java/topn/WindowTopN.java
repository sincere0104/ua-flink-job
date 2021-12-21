package topn;

import com.alibaba.fastjson.JSON;
import func.aggregate.SumAggregate;
import mockdata.Order;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import topn.bean.CityUserCostOfCurWindow;
import utils.KafkaConfigUtil;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * @ClassName WindowTopN
 * @Description TODO
 * @Author zby
 * @Date 2021-12-03 11:22
 * @Version 1.0
 * 5 分钟滚动窗口，根据订单交易日志，
 * 计算每个窗口内，每个城市，消费最高的 100 个用户
 * 假设数据延迟最大为 10s，即：WaterMark 最大延迟设置为 10s
 *
 * 比如第一个窗口的大小是[0,10) [10,20)
 * 当11这条数据来的时候就会触发定时器的逻辑，计算[0,10)的topN
 * 因为事件时间定时器的触发逻辑为： 当前 watermark >= timer 触发计算
 *
 *
 *
 **/
public class WindowTopN {

    private static final String KAFKA_CONSUMER_GROUP_ID = "console-consumer-93645";

    private static final String JOB_NAME = KAFKA_CONSUMER_GROUP_ID;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取订单数据，读取的是 json 类型的字符串
        FlinkKafkaConsumerBase<String> consumerBigOrder =
                new FlinkKafkaConsumer<>("order_topic_name",
                        new SimpleStringSchema(),
                        KafkaConfigUtil.buildConsumerProps(KAFKA_CONSUMER_GROUP_ID))
                        .setStartFromGroupOffsets();

        // 读取订单数据，从 json 解析成 Order 类，
        SingleOutputStreamOperator<Order> orderStream = env.addSource(consumerBigOrder)
                // 有状态算子一定要配置 uid
                .uid("order_topic_name")
                // 过滤掉 null 数据
                .filter(Objects::nonNull)
                // 将 json 解析为 Order 类
                .map(str -> JSON.parseObject(str, Order.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                            @Override
                            public long extractTimestamp(Order order, long recordTimestamp) {
                                return order.getTs();
                            }
                        }));

        SingleOutputStreamOperator<CityUserCostOfCurWindow> res = orderStream
                //根据city和user进行分组
                .keyBy((KeySelector<Order, Tuple2<Integer, String>>) order -> Tuple2.of(order.getCityId(), order.getUserId()))
                //开5分钟的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                // SumAggregate求窗口的累计值
                // CityUserCostWindowFunction 可以获取窗口的开始和结束时间，获取到每个窗口的结束时间后，下面再按照windowEnd和cityId keyBy
                .aggregate(new SumAggregate<>(Order::getPrice), new CityUserCostWindowFunction())
                .keyBy((KeySelector<CityUserCostOfCurWindow, Tuple2<Long, Integer>>)
                        entry -> Tuple2.of(entry.getTime(), entry.getCityId()))
                .process(new CityUserCostTopN());

        res.print();

        env.execute(JOB_NAME);
    }

    /**
     * 将 按照 city 和 user 当前窗口对应 cost 组合好发送到下游
     */
    private static class CityUserCostWindowFunction implements
            WindowFunction<Long, CityUserCostOfCurWindow,
                    Tuple2<Integer, String>, TimeWindow> {
        @Override
        public void apply(Tuple2<Integer, String> key,
                          TimeWindow window,
                          Iterable<Long> input,
                          Collector<CityUserCostOfCurWindow> out)
                throws Exception {
            long cost = input.iterator().next();
            out.collect(new CityUserCostOfCurWindow(window.getEnd(), key.f0, key.f1, cost));
        }
    }

    // 计算当前窗口，当前城市，消耗最多的 N 个用户
    private static class CityUserCostTopN extends
            KeyedProcessFunction<Tuple2<Long, Integer>,
                    CityUserCostOfCurWindow, CityUserCostOfCurWindow> {
        // 默认求 Top 100
        int n = 100;

        public CityUserCostTopN(int n) {
            this.n = n;
        }

        public CityUserCostTopN() {
        }

        // 存储当前窗口的所有用户的数据，待收齐同一个窗口的数据后，再触发 Top N 计算
        private ListState<CityUserCostOfCurWindow> entryState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<CityUserCostOfCurWindow> itemViewStateDesc = new ListStateDescriptor<>(
                    "entryState", CityUserCostOfCurWindow.class);
            entryState = getRuntimeContext().getListState(itemViewStateDesc);
        }

        /**
         * TODO 这个地方能否用小堆顶？ https://blog.csdn.net/wufaliang003/article/details/82940218
         * @param entry
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(CityUserCostOfCurWindow entry,
                                   Context ctx,
                                   Collector<CityUserCostOfCurWindow> out) throws Exception {
            // 在这里根本不知道当前窗口的数据是否全到了，所以只好利用 onTimer 来做
            entryState.add(entry);
            //注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收集好了所有 windowEnd的商品数据
            ctx.timerService().registerEventTimeTimer(entry.getTime() + 1);
        }

        /**
         * 定时器触发时，求前100
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<CityUserCostOfCurWindow> out) throws Exception {
            ArrayList<CityUserCostOfCurWindow> allEntry = new ArrayList<>();
            for (CityUserCostOfCurWindow item : entryState.get()) {
                allEntry.add(item);
            }
            //排序,求前100,用的是随机选择 + partition 的算法 时间复杂度 O(n)
            sortTopN(allEntry, new CostComparator(), n);
            for (int i = 0; i < n; i++) {
                out.collect(allEntry.get(i));
            }
            entryState.clear();
        }


        // 将前 N 个最大的元素，放到
        private static <T> void sortTopN(List<T> list, Comparator<? super T> c, int N) {
            T[] array = (T[]) list.toArray();
            int L = 0;
            int R = list.size() - 1;
            while (L != R) {
                int partition = recSortTopN(array, L, R, c);
                // 第 N 个位置已经拍好序
                if (partition == N) {
                    return;
                } else if (partition < N) {
                    L = partition + 1;
                } else {
                    R = partition - 1;
                }
            }
        }

        private static <E> int recSortTopN(E[] array, int L, int R,
                                           Comparator<? super E> c) {
            // 将 L mid R 三个位置的中位数的 index 返回，并将其交换到 L 的位置
            int mid = getMedian(array, L, R, c);
            if (mid != L) {
                swap(array, L, mid);
            }

            // 小的放左边，大的放右边
            E pivot = array[L];
            int i = L;
            int j = R;
            while (i < j) {
                // i 位置小于 等于 pivot，则 i 一直右移
                while (c.compare(array[i], pivot) <= 0 && i < j) {
                    i++;
                }
                // j 位置大于 等于 pivot，则 j 一直左移
                while (c.compare(array[i], pivot) <= 0 && i < j) {
                    j--;
                }
                if (i < j) {
                    swap(array, i, j);
                }
            }
            swap(array, L, i);
            return i;
        }


        private static <E> void swap(E[] array, int i, int j) {
            E tmp = array[i];
            array[i] = array[j];
            array[j] = tmp;
        }


        private static <E> int getMedian(E[] array, int L, int R,
                                         Comparator<? super E> c) {
            int mid = L + ((R - L) >> 1);
            // 拿到三个元素的值，返回中间元素 的 index
            E valueL = array[L];
            E valueMid = array[mid];
            E valueR = array[R];
            if (c == null) {
                return 0;
            }
            //R > M > L
            if (c.compare(valueL, valueMid) <= 0 && c.compare(valueMid, valueR) <= 0) {
                return mid;
            // R > L > M
            } else if (c.compare(valueMid, valueL) <= 0 && c.compare(valueL, valueR) <= 0) {
                return L;
            } else {
                return R;
            }
        }
    }

    private static class CostComparator implements Comparator<CityUserCostOfCurWindow> {
        @Override
        public int compare(CityUserCostOfCurWindow o1, CityUserCostOfCurWindow o2) {
            long diff = o2.getCost() - o1.getCost();
            return diff == 0 ? 0 : diff > 0 ? 1 : -1;
        }
    }
}
