package processfunc;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
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
 * @ClassName CurrentWatermarkBugDemo
 * @Description TODO
 * user2,0,张三,100.0
 * user2,1,李四,100.0
 * user2,2,王五,100.0
 * user2,3,王五,100.0
 * user2,4,王五,100.0
 * user1,5,赵六,100.0
 * user1,6,小七,100.0
 * user2,9,小九,100.0
 * user2,10,小十,100.0
 * user2,11,小十一,100.0
 * 注册的事件时间定时器能按照 wm >= 定时器时间触发，但是获取的wm是不正确的，内部处理的时候是正确的，什么原因？
 * currentWatermark,dataTime
 * -9223372036854775808,0
 * currentWatermark,dataTime
 * -1,1
 * currentWatermark,dataTime
 * 0,2
 * currentWatermark,dataTime
 * 1,3
 * currentWatermark,dataTime
 * 2,4
 * currentWatermark,dataTime
 * 3,5
 * currentWatermark,dataTime
 * 4,6
 * 5ms的定时器触发计算
 * @Author zby
 * @Date 2021-12-07 13:24
 * @Version 1.0
 **/
public class CurrentWatermarkBugDemo {

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
                .process(new KeyedProcessFunction<String, Tuple4<String, Long, String, Double>, Double>() {

                    long l = 0L;

                    @Override
                    public void processElement(Tuple4<String, Long, String, Double> value, Context ctx, Collector<Double> out) throws Exception {


                        long currentWatermark = ctx.timerService().currentWatermark();

                        Long timestamp = ctx.timestamp();

                        System.out.println("currentWatermark" + "," + "dataTime" + "," + timestamp);
                        System.out.println(currentWatermark + "," + value.f1 + "," + timestamp);

                        if (l == 0) {
                            ctx.timerService().registerEventTimeTimer(value.f1 + 5);
                            l = value.f1 + 5;
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Double> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);

                        System.out.println(timestamp + "ms的定时器触发计算");
                    }
                });


        //输出结果
        sum.print();

        env.execute("CurrentWatermarkBugDemo");
    }

}
