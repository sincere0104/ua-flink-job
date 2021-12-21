package processfunc;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName WindowTriggerDemo
 * @Description TODO 定义10ms的窗口，设置wm为0，问：当下面哪毫秒数据到来的时候，会触发窗口的计算？
 * 答案是：当第10ms数据到来的时候回触发窗口的计算，窗口为 [ 0, 10 ),currentWatermark = 10 - 0 - 1 = 9
 * wm生成源码：output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
 * 窗口触发源码：
 * public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
 * 	    //window.maxTimestamp() = windowEnd - 1
 * 	    //ctx.getCurrentWatermark() = maxTimestamp - outOfOrdernessMillis - 1
 * 		if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
 * 			// if the watermark is already past the window fire immediately
 * 			return TriggerResult.FIRE;
 *                } else {
 * 			ctx.registerEventTimeTimer(window.maxTimestamp());
 * 			return TriggerResult.CONTINUE;
 *        }
 *     }
 * user2,0,张三,100.0
 * user2,1,李四,100.0
 * user2,2,王五,100.0
 * user2,5,赵六,100.0
 * user2,6,小七,100.0
 * user2,9,小九,100.0
 * user2,10,小十,100.0
 * user2,11,小十一,100.0
 * @Author zby
 * @Date 2021-12-07 13:24
 * @Version 1.0
 **/
public class WindowTriggerDemo {

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

        env.execute("WindowTriggerDemo");
    }

}
