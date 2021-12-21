package window.windowtype;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName EventTimeSessionWindowsDemo
 * @Description TODO
 * 在普通的翻滚窗口和滑动窗口中，窗口的范围是按时间区间固定的，虽然范围有可能重合，但是处理起来
 * 是各自独立的，并不会相互影响。但是会话窗口则不同，其范围是根据事件之间的时间差是否超过gap来
 * 确定的（超过gap就形成一个新窗口），也就是说并非固定。所以，我们需要在每个事件进入会话窗口算
 * 子时就为它分配一个初始窗口，起点是它本身所携带的时间戳（这里按event time处理），终点则是时
 * 间戳加上gap的偏移量。这样的话，如果两个事件所在的初始窗口没有相交，说明它们属于不同的会话；
 * 如果相交，则说明它们属于同一个会话，并且要把这两个初始窗口合并在一起，作为新的会话窗口。
 * AggregateFunction接口中的merge方法，就是专门给session窗口合并数据的。
 *
user2,0,张三,100.0
user2,3,王五,100.0
user2,6,王五,100.0
user2,7,王五,100.0
user2,8,张三,100.0
user2,2,王五,100.0
user2,4,王五,100.0
user2,5,王五,100.0
user2,9,王五,100.0
user2,10,张三,100.0
user2,11,李四,100.0
user2,14,王五,100.0
user2,15,王五,100.0
 * @Author zby
 * @Date 2021-12-06 10:56
 * @Version 1.0
 **/
public class EventTimeSessionWindowsDemo {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据
        DataStreamSource<String> input = env.socketTextStream("cdh-master-01", 8899);

        SingleOutputStreamOperator<Tuple4<String, Long, String, Double>> wmStream = input.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofMillis(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] s = element.split(",");
                        return Long.valueOf(s[1]);
                    }
                }))
                .map(new MapFunction<String, Tuple4<String, Long, String, Double>>() {
                    @Override
                    public Tuple4<String, Long, String, Double> map(String value) throws Exception {
                        String[] s = value.split(",");
                        return Tuple4.of(s[0],Long.valueOf(s[1]),s[2],Double.valueOf(s[3]));
                    }
                });
        //keyBy(0) 计算班级总成绩，下标0表示班级
        SingleOutputStreamOperator<Double> sum = wmStream
                .keyBy(value -> value.f0)
                //数据之间差距大于 2 的时候，会划分 session 窗口
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(2)))
                .process(new ProcessWindowFunction<Tuple4<String, Long, String, Double>, Double, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple4<String, Long, String, Double>> elements, Collector<Double> out) throws Exception {

                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long currentWatermark = context.currentWatermark();

                        System.out.println("window:" + "[" + start + "," + end + ")");

                        System.out.println("currentWatermark:" + currentWatermark);

                        double sum = 0.0;

                        for (Tuple4<String, Long, String, Double> element : elements) {
                            sum = sum + element.f3;
                        }

                        out.collect(sum);
                    }
                });
        //输出结果
        //(class1,1638761400000,张三,300)
        //(class1,1638761700000,赵六,500)
        // 最后一条数据 2021-12-06 11:43:00 属于下个窗口，并没有打印，需要下个有效事件到来的时候，才能触发计算
        sum.print();

        env.execute("EventTimeSessionWindowsDemo");
    }

    /**
     * 定义班级的三元数组
     */
    public static final Tuple4[] ENGLISH = new Tuple4[]{
            //班级 姓名 成绩
            //2021-12-06 11:30:00
            Tuple4.of("class1",1638761400000L,"张三",100L),
            //2021-12-06 11:31:00
            Tuple4.of("class1",1638761460000L,"李四",100L),
            //2021-12-06 11:32:00
            Tuple4.of("class1",1638761520000L,"王五",100L),
            //2021-12-06 11:35:00
            Tuple4.of("class1",1638761700000L,"赵六",100L),
            //2021-12-06 11:36:00
            Tuple4.of("class1",1638761760000L,"小七",100L),
            //2021-12-06 11:37:00
            Tuple4.of("class1",1638761820000L,"小八",100L),
            //2021-12-06 11:38:00
            Tuple4.of("class1",1638761880000L,"小九",100L),
            //2021-12-06 11:40:00
            Tuple4.of("class1",1638762000000L,"小十",100L),
            //2021-12-06 11:43:00
            Tuple4.of("class1",1638762180000L,"小十一",100L),
    };
}
