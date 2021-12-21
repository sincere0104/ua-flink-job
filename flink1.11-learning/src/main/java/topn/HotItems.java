package topn;

import func.aggregate.CountAggregate;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import topn.bean.UserBehavior;


/**
 * @ClassName HotItems
 * @Description TODO 每隔5分钟输出一下点击量最多的前 N 个商品。
 * 2021-12-09 11:00:00
 * 1,1,1,pv,1639018800
 * 1min
 * 1,1,1,pv,1639018860
 * 3min
 * 1,1,1,pv,1639018980
 * 5min
 * 1,1,1,pv,1639019100
 * 6min      触发计算topN的计算逻辑
 * 1,1,1,pv,1639019160
 * 7min
 * 1,1,1,pv,1639019220
 *
 *
 * 1,1,1,pv,0
 * 1,1,1,pv,1
 * 1,1,1,pv,3
 * 1,1,1,pv,5
 * 1,1,1,pv,6
 * 1,1,1,pv,7  当是5ms的窗口时，7ms数据到来的时候才会触发计算topN的计算逻辑
 *
 *
 * @Author zby
 * @Date 2021-12-09 09:53
 * @Version 1.0
 **/
public class HotItems {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> streamSource = env.socketTextStream("cdh-master-01", 8899);


        DataStream<UserBehavior> timedData = streamSource
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] s = value.split(",");
                        return new UserBehavior(Long.valueOf(s[0]),Long.valueOf(s[1]),Integer.valueOf(s[2]),s[3],Long.valueOf(s[4]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                //升序
                .<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        // 原始数据单位秒，将其转成毫秒
                        //return element.timestamp * 1000;
                        return element.timestamp;
                    }
                }));

        SingleOutputStreamOperator<String> topItems = timedData
                .filter(userBehavior -> userBehavior.behavior.equals("pv"))
                .keyBy(value -> value.itemId)
                //5分钟的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                //.window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                .aggregate(new CountAggregate<UserBehavior>(), new WindowResultFunction())
                .keyBy(value -> value.windowEnd)
                //求点击量前3名的商品
                .process(new TopNHotItems(1));

        topItems.print();
        env.execute("Hot Items Job");

    }
}
