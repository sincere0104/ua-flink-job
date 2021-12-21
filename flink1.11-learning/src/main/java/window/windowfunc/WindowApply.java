package window.windowfunc;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName WindowApply
 * @Description TODO WindowFunction也是全量聚合函数，已被更高级的ProcessWindowFunction逐渐代替
 *              做个累加操作和ProcessWindowFunction 处理逻辑一样
 * @Author zby
 * @Date 2021-12-03 18:28
 * @Version 1.0
 **/
public class WindowApply {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //读取数据
        DataStream<Tuple3<String, String, Long>> input = env.fromElements(ENGLISH);

        //keyBy(0) 计算班级总成绩，下标0表示班级
        //countWindow(3) 根据元素个数对数据流进行分组切片，达到3个，触发窗口进行计算
        SingleOutputStreamOperator<Double> totalPoints = input
                .keyBy(value -> value.f0)
                .countWindow(3)
                .apply(new WindowFunction<Tuple3<String, String, Long>, Double, String, GlobalWindow>() {

                    @Override
                    public void apply(String s, GlobalWindow window, Iterable<Tuple3<String, String, Long>> input, Collector<Double> out) throws Exception {
                        long sum = 0;
                        long count = 0;
                        for (Tuple3<String, String, Long> in : input) {
                            sum += in.f2;
                            count++;
                        }
                        out.collect((double) (sum / count));
                    }
                });

        totalPoints.print();

        env.execute("WindowFunctionApply");
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
