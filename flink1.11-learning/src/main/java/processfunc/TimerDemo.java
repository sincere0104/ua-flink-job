package processfunc;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName TimerDemo
 * @Description TODO
 * //指标类型 : A
 * //指标类型名称: 特定事件触发后统计
 * //场景: 转出金额<100后，30分钟内再累计转出 20000
 * //指标返回结果: 返回统计的指标数值
 * @Author zby
 * @Date 2021-12-07 13:24
 * @Version 1.0
 **/
public class TimerDemo {

    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据
        DataStreamSource<String> input = env.socketTextStream("cdh-master-01", 8899);

        //抽取wm
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
                        return Tuple4.of(s[0],Long.valueOf(s[1]),s[2],Double.valueOf(s[3]));
                    }
                });


        //keyBy(0) 计算班级总成绩，下标0表示班级
        SingleOutputStreamOperator<Double> process = wmStream
                .keyBy(value -> value.f0)
                .process(new KeyedProcessFunction<String, Tuple4<String, Long, String, Double>, Double>() {
                    //保存金额累计值
                    private transient ValueState<Double> sumState;
                    //是否注册定时器
                    private transient ValueState<Boolean> isRegisterTimerState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Double> sumStateDescriptor = new ValueStateDescriptor<>("sumState", TypeInformation.of(Double.class));
                        sumState = getRuntimeContext().getState(sumStateDescriptor);
                        ValueStateDescriptor<Boolean> isRegisterTimerStateDescriptor = new ValueStateDescriptor<>("isRegisterTimerState", TypeInformation.of(Boolean.class));
                        isRegisterTimerState = getRuntimeContext().getState(isRegisterTimerStateDescriptor);
                    }

                    @Override
                    public void processElement(Tuple4<String, Long, String, Double> value, Context ctx, Collector<Double> out) throws Exception {

                        /**
                         *
                         * if ( 没有存在的定时器 && 当前金额 < 100){
                         *  1、将数据保存到一个新状态里
                         *  2、注册30分钟之后触发的定时器
                         *}
                         * if ( 存在的定时器 ){
                         *     直接将数据保存到状态里，不管金额是否大于100
                         *}
                         * if （ 没有存在的定时器 && 当前金额 > 100）{
                         *     此时什么也不做
                         * }
                         *
                         * if ( 没有存在的定时器 ) {
                         *     if ( 当前金额 < 100 ) {
                         *          1、将数据保存到一个新状态里
                         *          2、注册30分钟之后触发的定时器
                         *     } else {
                         *          此时什么也不做
                         *     }
                         * } else {
                         *     直接将数据保存到状态里，不管金额是否大于100
                         * }
                         *
                         * onTimer里面
                         *  1、清除定时器
                         *  2、清除状态数据
                         *
                         */
                        //初始化
                        if (sumState.value() == null) {
                            sumState.update(0.0);
                        }
                        //初始化
                        if (isRegisterTimerState.value() == null) {
                            isRegisterTimerState.update(false);
                        }

                        //获取状态值
                        Boolean isRegisterTimerFlag = isRegisterTimerState.value();
                        Double sum = sumState.value();

                        //
                        Double money = value.f3;

                        long currentWatermark = ctx.timerService().currentWatermark();

                        System.out.println(currentWatermark);


                        if (!isRegisterTimerFlag) {
                            if (money < 100) {

                                sum = sum + money;


                                //long timeTimer = ctx.timerService().currentProcessingTime() + 1000;
                                //ctx.timerService().registerProcessingTimeTimer(timeTimer);

                                long timeTimer = ctx.timestamp() + 120000L;
                                ctx.timerService().registerEventTimeTimer(timeTimer);

                                sumState.update(sum);
                                isRegisterTimerState.update(true);


                            } else {
                                System.out.println("没有注册定时器，并且金额 > 100");
                            }
                        } else {
                            sum = sum + money;
                            sumState.update(sum);
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Double> out) throws Exception {

                        System.out.println("我被触发了....");
                        out.collect(sumState.value());
                        isRegisterTimerState.update(false);
                        sumState.update(0.0);
                        // 直接清除状态
                        //isRegisterTimerState.clear();
                        //sumState.clear();


                    }
                });

        //输出结果
        process.print();

        env.execute("TimerDemo");
    }

}
