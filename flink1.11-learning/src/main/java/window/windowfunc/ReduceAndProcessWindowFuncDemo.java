package window.windowfunc;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName ReduceAndProcessWindowFuncDemo
 * @Description TODO 既可以增量聚合，也可以访问窗口的元数据信息(如开始结束时间、状态等)。
 * ReduceFunction/AggregateFunction和ProcessWindowFunction结合使用，分配到某个窗口的元素将被提前聚合，
 * 而当窗口的trigger触发时，也就是窗口收集完数据关闭时，将会把聚合结果发送到ProcessWindowFunction中，
 * 这时Iterable参数将会只有一个值，就是前面聚合的值。
 * @Author zby
 * @Date 2021-12-03 18:15
 * @Version 1.0
 **/
public class ReduceAndProcessWindowFuncDemo {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //读取数据
        DataStream<Tuple3<String, String, Integer>> input = env.fromElements(ENGLISH);

        //keyBy(0) 计算班级总成绩，下标0表示班级
        //countWindow(6) 根据元素个数对数据流进行分组切片，达到6个，触发窗口进行计算
        DataStream<Tuple3<String, String, Integer>> totalPoints = input
                .keyBy(value -> value.f0)
                .countWindow(6)
                .reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1, Tuple3<String, String, Integer> value2) throws Exception {
                        //效果如下：
                        //(class1,张三,100)
                        //(class1,李四,30)
                        //==============
                        System.out.println("" + value1);
                        System.out.println("" + value2);
                        System.out.println("==============");
                        return new Tuple3<>(value1.f0, value1.f1, value1.f2 + value2.f2);
                    }
                }, new ProcessWindowFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>, String, GlobalWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple3<String, String, Integer>> elements, Collector<Tuple3<String, String, Integer>> out) throws Exception {

                        Tuple3<String, String, Integer> next = elements.iterator().next();

                        //context 中有窗口的信息
                        out.collect(next);
                    }
                });

        //输出结果
        //TODO 中间结果是进行累加的，等到窗口触发的时候，才会输出中间结果
        //效果如下：
        //(class1,张三,300)
        totalPoints.print();

        env.execute("ReduceFunctionDemo");
    }

    /**
     * 定义班级的三元数组
     */
    public static final Tuple3[] ENGLISH = new Tuple3[]{
            //班级 姓名 成绩
            Tuple3.of("class1", "张三", 100),
            Tuple3.of("class1", "李四", 30),
            Tuple3.of("class1", "王五", 70),
            Tuple3.of("class1", "赵六", 50),
            Tuple3.of("class1", "小七", 40),
            Tuple3.of("class1", "小八", 10),
            Tuple3.of("class2", "赵六", 50),
            Tuple3.of("class2", "小七", 40),
            Tuple3.of("class2", "小八", 10),
            Tuple3.of("class2", "小九", 10),
    };
}
