import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

public class DimModelExtract {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Source：从 kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka-server-01:9092,kafka-server-02:9092,kafka-server-03:9092");
        properties.setProperty("group.id", "group-dim-model");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "");

        DataStreamSource<String> kafkaSource = env.addSource(
                new FlinkKafkaConsumer<String>(
                        "event-insert",
                        new SimpleStringSchema(),
                        properties).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .forBoundedOutOfOrderness(Duration.ofSeconds(20)))
        );

        DataStream<DimBean> map = kafkaSource.map(new MapFunction<String, DimBean>() {
            @Override
            public DimBean map(String value) throws Exception {
                Gson gson = new Gson();
                EventInsert eventInsert = gson.fromJson(value, EventInsert.class);
                //BasicProp basicProp = eventInsert.getBasicProp();
                ExtendProp extendProp = eventInsert.getExtendProp();

                return new DimBean(extendProp.getModel(), extendProp.getModel());
            }
        });

        SingleOutputStreamOperator<DimBean> mapState = map.keyBy(r -> r.getModelId())
                .process(new KeyedProcessFunction<String, DimBean, DimBean>() {

                    MapState<String, String> modelMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        modelMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, String>
                                ("modelMapState", String.class, String.class));
                    }

                    @Override
                    public void processElement(DimBean value, Context ctx, Collector<DimBean> out) throws Exception {

                        // 判断操作系统是否已在状态中,不在则加入
                        if (!modelMapState.contains(value.getModelId())) {

                            modelMapState.put(value.getModelId(), ""); // 添加单个 k-v 对
                            out.collect(value);
                        }

                    }
                }).uid("DimModelExtract-001");

        FlinkKafkaProducer kafkaProducer = new FlinkKafkaProducer("dim-model",
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );

        //写入到kafka
        mapState.map(value -> value.getModelId() + "&" + value.getModelId()).addSink(kafkaProducer);

//        mapState.map(new MapFunction<DimBean, Object>() {
//            @Override
//            public Object map(DimBean value) throws Exception {
//                Gson gson = new Gson();
//                return gson.toJson(value);
//            }
//        }).addSink(kafkaProducer);

        env.execute("dim_model");
    }
}
