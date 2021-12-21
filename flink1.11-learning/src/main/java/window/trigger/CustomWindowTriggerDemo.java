package window.trigger;

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
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName CustomWindowTriggerDemo
 * @Description TODO 根据次数和和窗口结束时间，共同触发窗口的计算
 * @Author zby
 * @Date 2021-12-13 15:11
 * @Version 1.0
 **/
public class CustomWindowTriggerDemo {
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
                //.trigger()
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

        env.execute("CustomWindowTriggerDemo");
    }

    private static class MyCustomTrigger extends Trigger<Tuple4,TimeWindow>{

        public MyCustomTrigger() {
            super();
        }

        @Override
        public TriggerResult onElement(Tuple4 element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public boolean canMerge() {
            return super.canMerge();
        }

        @Override
        public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
            super.onMerge(window, ctx);
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }
}
