package window.windowtype;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

/**
 * @ClassName GlobalWindowsDemo
 * @Description TODO 全局窗口(GlobalWindow)的默认触发器是永不触发的NeverTrigger。
 * 全局窗口分配器将所有具有相同key的元素分配到同一个全局窗口中，
 * 这个窗口模式仅适用于用户还需自定义触发器的情况。否则，由于全局窗口没有一个自然的结尾，
 * 无法执行元素的聚合，将不会有计算被执行。
 * @Author zby
 * @Date 2021-12-06 10:38
 * @Version 1.0
 **/
public class GlobalWindowsDemo {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //读取数据
        DataStream<Tuple3<String, String, Long>> input = env.fromElements(ENGLISH);

        //keyBy(0) 计算班级总成绩，下标0表示班级
        SingleOutputStreamOperator<Tuple3<String, String, Long>> sum = input
                .keyBy(value -> value.f0)
                .window(GlobalWindows.create())
                //必需要有窗口触发器 没有的话，窗口不会执行计算逻辑
                .trigger(CountTrigger.of(3))
                .sum(2);

        sum.print();

        env.execute("GlobalWindowsDemo");
    }

    /**
     * 定义班级的三元数组
     */
    public static final Tuple3[] ENGLISH = new Tuple3[]{
            //班级 姓名 成绩
            Tuple3.of("class1","张三",100L),
            Tuple3.of("class1","李四",78L),
            Tuple3.of("class1","王五",99L),
            Tuple3.of("class2","赵六",81L),
            Tuple3.of("class2","小七",59L),
            Tuple3.of("class2","小八",97L),
    };
}
