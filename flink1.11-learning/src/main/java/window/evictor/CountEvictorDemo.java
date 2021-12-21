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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName CountEvictorDemo
 * @Description TODO 默认在窗口函数执行前调用 如果当前窗口元素个数小于等于用户指定的阈值则不做删除操作，否则会从窗口迭代器的头部开始删除多余的元素(size - maxCount)
 * TODO 注意:Flink不保证窗口中元素的顺序。这意味着，尽管逐出器可以从窗口的开头移除元素，但这些元素不一定是最先到达或最后到达的元素。
 * 在window中使用Evictor操作的时候，无论window是否有reduce等其他算子，window一律缓存窗口的所有数据，
 * 等到窗口触发的时候先执行evictor方法，再执行reduce，最后再执行ProcessWindowFunction操作，源代码是
 * 通过EvictingWindowOperator这个类来实现的对比没有Evictor时效率低了很多，如果窗口缓存的数据量很大的
 * 话也会导致OOM的发生，使用Evictor时要谨慎！
 * user2,0,张三,100.0
 * user2,1,李四,100.0
 * user2,4,王五,100.0
 * user2,5,赵六,100.0
 * user2,10,小十,100.0
 * 输出结果：
 * window:[0,10]
 * currentWatermark:9
 * window中包含的数据：
 * (user2,4,王五,100.0)
 * (user2,5,赵六,100.0)
 * 200.0
 * @Author zby
 * @Date 2021-12-09 15:19
 * @Version 1.0
 **/
public class CountEvictorDemo {

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
                // ProcessWindowFunction 触发前，删除多余的元素
                .evictor(CountEvictor.of(2))
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

        env.execute("CountEvictorDemo");
    }

}
