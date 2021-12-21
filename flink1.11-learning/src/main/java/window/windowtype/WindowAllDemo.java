package window.windowtype;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @ClassName WindowAllDemo
 * @Description TODO Non-keyed window 和 keyed window 唯一的区别就是：所有数据将发送给下游的单个实例，或者说下游算子的并行度为1。
 * @Author zby
 * @Date 2021-12-06 14:40
 * @Version 1.0
 **/
public class WindowAllDemo {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //读取数据
        DataStream<Tuple4<String,Long,String, Long>> input = env.fromElements(ENGLISH);

        //抽取wm
        SingleOutputStreamOperator<Tuple4<String, Long, String, Long>> wmStream = input.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple4<String, Long, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, Long, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String, Long, String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                }));

        SingleOutputStreamOperator<Tuple4<String,Long,String, Long>> sum = wmStream
                .windowAll(EventTimeSessionWindows.withGap(Time.minutes(2)))
                //数据之间差距大于 2 的时候，会划分 session 窗口
                .sum(3);

        sum.print();

        System.out.println(env.getExecutionPlan());

        env.execute("WindowAllDemo");
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
