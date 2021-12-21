package window.allowedlateness;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName allowedLatenessDemo
 * 晚到但是未丢弃的元素可能会导致窗口再次被触发。
 * 比如 [ 0ms,10ms)的窗口，本来是wm >= window end - 1 的时候就会触发窗口的计算，要是
 * allowedLateness 不为0的话，假设为2，那么之后来的数据，只要小于12，就都会触发窗口的再次计算。
 *
 *
 * 相当于watermark的计算规则变成 : maxTimestamp - outOfOrdernessMillis - 1 - allowedLateness
 *
 * @Description TODO
 * user2,0,张三,100.0
 * user2,1,李四,100.0
 * user2,2,王五,100.0
 * user2,3,王五,100.0
 * user2,4,王五,100.0
 * user2,5,赵六,100.0
 * user2,10,小十,100.0
 * user2,11,小九,100.0 wm = 10
 * user2,5,赵六,100.0 再次触发计算
 * user2,12,小九,100.0 wm = 11
 * user2,7,小七,100.0 不会再次触发窗口的计算
 * user2,4,王五,100.0 不会再次触发窗口的计算
 * @Author zby
 * @Date 2021-12-09 18:10
 * @Version 1.0
 **/
public class allowedLatenessDemo {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据
        DataStreamSource<String> input = env.socketTextStream("cdh-master-01", 8899);

        SingleOutputStreamOperator<Tuple4<String, Long, String, Double>> wmStream = input.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
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


        SingleOutputStreamOperator<Double> sum = wmStream
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
                .allowedLateness(Time.milliseconds(2))
                .process(new ProcessWindowFunction<Tuple4<String, Long, String, Double>, Double, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple4<String, Long, String, Double>> elements, Collector<Double> out) throws Exception {


                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long currentWatermark = context.currentWatermark();

                        System.out.println("window:" + "[" + start + "," + end + "]");

                        System.out.println("currentWatermark:" + currentWatermark);

                        double sum = 0.0;

                        for (Tuple4<String, Long, String, Double> element : elements) {
                            sum = sum + element.f3;
                        }

                        out.collect(sum);
                    }
                });


        //输出结果
        sum.print();

        env.execute("allowedLatenessDemo");
    }
}
