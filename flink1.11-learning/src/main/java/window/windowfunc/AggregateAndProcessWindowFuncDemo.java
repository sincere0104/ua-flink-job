package window.windowfunc;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName AggregateAndProcessWindowFuncDemo
 * @Description TODO 既可以增量聚合，也可以访问窗口的元数据信息(如开始结束时间、状态等)。
 * @Author zby
 * @Date 2021-12-03 18:15
 * @Version 1.0
 **/
public class AggregateAndProcessWindowFuncDemo {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 读取数据
        DataStream<Tuple3<String, String, Long>> input = env.fromElements(ENGLISH);

        // 求各个班级英语成绩平均分
        DataStream<Double> avgScore = input
                .keyBy(value -> value.f0)
                .countWindow(4)
                .aggregate(new AverageAggregate(), new MyProcessWindowFunction());

        avgScore.print();

        env.execute("AggregateFunctionDemo");

    }

    public static final Tuple3[] ENGLISH = new Tuple3[] {
            Tuple3.of("class1", "张三", 100L),
            Tuple3.of("class1", "李四", 40L),
            Tuple3.of("class1", "王五", 60L),
            Tuple3.of("class1", "赵六", 20L),
            Tuple3.of("class2", "王五", 60L),
            Tuple3.of("class2", "赵六", 20L),
            Tuple3.of("class2", "小七", 30L),
            Tuple3.of("class2", "小八", 50L),
    };

    /**
     * Tuple3<String, String, Long> 输入类型
     * Tuple2<Long, Long> 累加器ACC类型，保存中间状态
     * Double 输出类型
     */
    public static class AverageAggregate implements AggregateFunction<Tuple3<String, String, Long>, Tuple2<Long, Long>, Double> {
        /**
         * 创建累加器保存中间状态（sum count）
         * 窗口开始的时候回执行一次
         * sum 英语总成绩
         * count 学生个数
         *
         * @return
         */
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            System.out.println("createAccumulator");
            return new Tuple2<>(0L, 0L);
        }

        /**
         * 将元素添加到累加器并返回新的累加器
         *
         * @param value 输入类型
         * @param acc 累加器ACC类型
         * 窗口的每条数据都会调用此方法
         * @return 返回新的累加器
         */
        @Override
        public Tuple2<Long, Long> add(Tuple3<String, String, Long> value, Tuple2<Long, Long> acc) {
            //acc.f0 总成绩
            //value.f2 表示成绩
            //acc.f1 人数
            System.out.println("add");
            return new Tuple2<>(acc.f0 + value.f2, acc.f1 + 1L);
        }

        /**
         * 从累加器提取结果
         * 窗口结束的时候会运行一次
         * @param acc
         * @return
         */
        @Override
        public Double getResult(Tuple2<Long, Long> acc) {
            System.out.println("getResult");
            return ((double) acc.f0) / acc.f1;
        }

        /**
         * 累加器合并
         * AggregateFunction中的merge方法仅SessionWindow会调用该方法，如果time window是不会调用的，merge方法即使返回null也是可以的
         * 官网中的描述大概的意思是：因为会话窗口没有固定的起始时间和结束时间，他们被运算不同于滚动窗口和滑动窗口。
         * 本质上，会话窗口会为每一批相邻两条数据没有大于指定间隔时间的数据merge到以一起。为了数据能够被merge，
         * 会话窗口需要一个merge的触发器和一个可以merge的WindowFunction，比如ReduceFunction、AggregateFunction
         * 或者ProcessWindowFunction，需要注意的是FoldFunction不能merge！
         * @param acc1
         * @param acc2
         * @return
         */
        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> acc1, Tuple2<Long, Long> acc2) {
            System.out.println("merge");
            return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
        }
    }

    /**
     *
     */
    public static class MyProcessWindowFunction extends ProcessWindowFunction<Double, Double, String, GlobalWindow>{
        @Override
        public void process(String s, Context context, Iterable<Double> elements, Collector<Double> out) throws Exception {
            Double next = elements.iterator().next();

            out.collect(next);
        }
    }
}
