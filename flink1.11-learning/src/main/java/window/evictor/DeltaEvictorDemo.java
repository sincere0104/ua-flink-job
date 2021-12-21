package window.evictor;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName DeltaEvictorDemo
 * @Description TODO
 * 根据用户自定的 DeltaFunction 函数来计算窗口中最后一个元素与其余每个元素之间的差值，
 * 如果差值大于等于用户指定的阈值就会删除该元素。
 * user2,0,张三,101.0
 * user2,1,李四,100.0 被删除
 * user2,3,李四,90.0  被删除
 * user2,4,王五,110.0
 * user2,5,赵六,200.0
 * user2,10,小十,100.0
 * 输出结果：
 * window:[0,10]
 * currentWatermark:9
 * window中包含的数据：
 * (user2,0,张三,101.0)
 * (user2,4,王五,110.0)
 * (user2,5,赵六,200.0)
 * 411.0
 * @Author zby
 * @Date 2021-12-13 13:09
 * @Version 1.0
 **/
public class DeltaEvictorDemo {
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
                        return Tuple4.of(s[0], Long.valueOf(s[1]), s[2], Double.valueOf(s[3]));
                    }
                });


        SingleOutputStreamOperator<Double> sum = wmStream
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
                // ProcessWindowFunction 触发前，删除和最后一个值差值 >= 100的数据
                .evictor(DeltaEvictor.of(100, new DeltaFunction<Tuple4<String, Long, String, Double>>() {
                    @Override
                    public double getDelta(Tuple4<String, Long, String, Double> oldDataPoint, Tuple4<String, Long, String, Double> newDataPoint) {
                        //newDataPoint 就是窗口中的最后一条数据
                        return newDataPoint.f3 - oldDataPoint.f3;
                    }
                }))
                // ProcessWindowFunction 触发后，删除多余的元素，我感觉几乎没有使用场景
                //.evictor(CountEvictor.of(2,true))
                .process(new ProcessWindowFunction<Tuple4<String, Long, String, Double>, Double, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple4<String, Long, String, Double>> elements, Collector<Double> out) throws Exception {


                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long currentWatermark = context.currentWatermark();

                        System.out.println("window:" + "[" + start + "," + end + "]");

                        System.out.println("currentWatermark:" + currentWatermark);

                        double sum = 0.0;

                        System.out.println("window中包含的数据：");
                        for (Tuple4<String, Long, String, Double> element : elements) {

                            System.out.println(element);

                            sum = sum + element.f3;
                        }

                        out.collect(sum);
                    }
                });


        //输出结果
        sum.print();

        env.execute("DeltaEvictorDemo");
    }
}
