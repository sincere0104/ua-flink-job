package window.windowtype;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @ClassName EventTimeSessionWindowsDemo
 * @Description TODO 并没有按照预想的结果进行输出
 * 在普通的翻滚窗口和滑动窗口中，窗口的范围是按时间区间固定的，虽然范围有可能重合，但是处理起来
 * 是各自独立的，并不会相互影响。但是会话窗口则不同，其范围是根据事件之间的时间差是否超过gap来
 * 确定的（超过gap就形成一个新窗口），也就是说并非固定。所以，我们需要在每个事件进入会话窗口算
 * 子时就为它分配一个初始窗口，起点是它本身所携带的时间戳（这里按event time处理），终点则是时
 * 间戳加上gap的偏移量。这样的话，如果两个事件所在的初始窗口没有相交，说明它们属于不同的会话；
 * 如果相交，则说明它们属于同一个会话，并且要把这两个初始窗口合并在一起，作为新的会话窗口。
 * AggregateFunction接口中的merge方法，就是专门给session窗口合并数据的。
 * @Author zby
 * @Date 2021-12-06 10:56
 * @Version 1.0
 **/
public class EventTimeSessionWindowsDynamicGapDemo {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //读取数据
        DataStream<Tuple5<String,Long,Integer,String, Long>> input = env.fromElements(ENGLISH);

        //抽取wm
        SingleOutputStreamOperator<Tuple5<String,Long,Integer,String, Long>> wmStream = input.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple5<String,Long,Integer,String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple5<String,Long,Integer,String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple5<String,Long,Integer,String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                }));

        //keyBy(0) 计算班级总成绩，下标0表示班级
        SingleOutputStreamOperator<Tuple5<String,Long,Integer,String, Long>> sum = wmStream
                .keyBy(value -> value.f0)
                //数据之间差距大于 2 的时候，会划分 session 窗口
                //.window(EventTimeSessionWindows.withGap(Time.minutes(2)))
                .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple5<String,Long,Integer,String, Long>>() {
                    /**
                     *
                     * @param element
                     * @return The session time gap in milliseconds.
                     */
                    @Override
                    public long extract(Tuple5<String,Long,Integer,String, Long> element) {
                        return element.f2;
                    }
                }))
                .sum(4);
        //输出结果
        sum.print();

        env.execute("EventTimeSessionWindowsDynamicGapDemo");
    }

    /**
     * 定义班级的三元数组
     */
    public static final Tuple5[] ENGLISH = new Tuple5[]{
            //班级 姓名 成绩
            //2021-12-06 11:30:00
            Tuple5.of("class1",1638761400000L,120000,"张三",100L),
            //2021-12-06 11:31:00
            Tuple5.of("class1",1638761460000L,120000,"李四",100L),
            //2021-12-06 11:32:00
            Tuple5.of("class1",1638761520000L,120000,"王五",100L),
            //2021-12-06 11:35:00
            Tuple5.of("class1",1638761700000L,240000,"赵六",100L),
            //2021-12-06 11:37:00
            //Tuple5.of("class1",1638761820000L,240000,"小七",100L),
            //2021-12-06 11:38:00
            Tuple5.of("class1",1638761880000L,240000,"小九",100L),
            //2021-12-06 11:40:00
            Tuple5.of("class1",1638762000000L,240000,"小十",100L),
            //2021-12-06 11:43:00
            Tuple5.of("class1",1638762180000L,240000,"小十一",100L),
    };
}
