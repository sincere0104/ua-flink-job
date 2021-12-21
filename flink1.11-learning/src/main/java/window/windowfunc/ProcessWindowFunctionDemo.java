package window.windowfunc;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName ProcessWindowFunctionDemo
 * @Description TODO 将ProcessWindowFunction用于简单聚合（如count）效率很低。将ReduceFunction或AggregateFunction与ProcessWindowFunction组合，以获得ProcessWindowFunction的增量聚合和添加信息。
 * 如果ProcessWindowFunction没有结合reduce,aggregate等其他窗口函数来计算的话，
 * 他是会缓存落入该窗口的所有数据，等待窗口触发的时候再一起执行ProcessWindowFunction中的process方法的，
 * 如果数据量太大很可能会导致OOM，建议在ProcessWindowFunction之前加入一个reduce，aggregate等算子，
 * 这些算子会在数据落入窗口的时候就执行reduce等操作，而不是缓存直到窗口触发执行的时候才进行reduce操作，
 * 从而避免了缓存所有窗口数据，窗口触发的时候ProcessWindowFunction拿到的只是reduce操作后的结果。
 * @Author zby
 * @Date 2021-12-03 17:17
 * @Version 1.0
 **/
public class ProcessWindowFunctionDemo {
    public static void main(String[] args) throws Exception{
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //读取数据
        DataStream<Tuple3<String,String,Long>> input = env.fromElements(ENGLISH);

        //求各班级英语成绩平均分
        DataStream<Double> avgScore = input
                .keyBy(value -> value.f0)
                .countWindow(3)
                .process(new MyProcessWindowFunction());

        avgScore.print();
        env.execute("TestProcessWinFunctionOnWindow");

    }

    /**
     * 用 GlobalWindow 或者 TimeWindow
     */
    public static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String,String,Long>,Double, String, GlobalWindow> {


        @Override
        public void process(String s, Context context, Iterable<Tuple3<String, String, Long>> elements, Collector<Double> out) throws Exception {
            long sum = 0;
            long count = 0;
            for (Tuple3<String,String,Long> in :elements){
                sum+=in.f2;
                count++;
            }
            out.collect((double)(sum/count));
        }
    }


    public static final Tuple3[] ENGLISH = new Tuple3[]{
            Tuple3.of("class1","张三",100L),
            Tuple3.of("class1","李四",78L),
            Tuple3.of("class1","王五",99L),
            Tuple3.of("class2","赵六",81L),
            Tuple3.of("class2","小七",59L),
            Tuple3.of("class2","小八",97L),
    };
}
