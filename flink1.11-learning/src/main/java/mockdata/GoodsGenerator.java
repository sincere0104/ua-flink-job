package mockdata;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * @ClassName GoodsGenerator
 * @Description TODO
 * @Author zby
 * @Date 2021-12-03 10:26
 * @Version 1.0
 **/
public class GoodsGenerator extends RichParallelSourceFunction<Goods> {
    private static final Random RANDOM = new Random();
    private static int goodsId = 0;

    @Override
    public void run(SourceContext<Goods> sourceContext) throws Exception {
        while (true) {
            int goodsId = GoodsGenerator.goodsId++;

            Goods goods = new Goods(goodsId,"手机",false);
            sourceContext.collect(goods);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}
