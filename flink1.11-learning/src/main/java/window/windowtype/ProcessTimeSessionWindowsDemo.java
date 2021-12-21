package window.windowtype;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
/**
 * @ClassName ProcessTimeSessionWindowsDemo
 * @Description TODO
 * @Author zby
 * @Date 2021-12-06 11:01
 * @Version 1.0
 **/
public class ProcessTimeSessionWindowsDemo {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //读取数据
        //DataStream<Tuple4<String,Long,String, Long>> input = env.fromElements(ENGLISH);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 8888);

        //keyBy(0) 计算班级总成绩，下标0表示班级
        SingleOutputStreamOperator<Tuple4<String, Long, String, Long>> sum = streamSource
                .map(new MapFunction<String, Tuple4<String, Long, String, Long>>() {
                    @Override
                    public Tuple4<String, Long, String, Long> map(String value) throws Exception {
                        String[] s = value.split(",");
                        return Tuple4.of(s[0], Long.valueOf(s[1]), s[2], Long.valueOf(s[3]));
                    }
                })
                .keyBy(value -> value.f0)
                //数据之间差距大于 10s 的时候，会划分 session 窗口
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .sum(3);
        //输出结果
        //当是处理时间时，收到数据后，会以最后一条数据的处理时间+gap，进行划分窗口，
        //假如发送 class1,1638761400000,张三,100 后，10s后会输出 class1,1638761400000,张三,100
        sum.print();

        env.execute("ProcessingTimeSessionWindowsDemo");
    }

    /**
     * 定义班级的四元数组
     * 测试数据：
     * class1,1638761400000,张三,100
     * class1,1638761460000,李四,100
     * class1,1638761520000,王五,100
     * class1,1638761700000,赵六,100
     * class1,1638761760000,小七,100
     * class1,1638761820000,小八,100
     * class1,1638761880000,小九,100
     * class1,1638762000000,小十,100
     * class1,1638762180000,小十一,100
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
