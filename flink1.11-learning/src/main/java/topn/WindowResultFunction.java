package topn;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import topn.bean.ItemViewCount;

/**
 * @ClassName WindowResultFunction
 * @Description TODO
 * @Author zby
 * @Date 2021-12-09 10:35
 * @Version 1.0
 **/
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        @Override
        public void apply(
                Long key,  // 窗口的主键，即 itemId
                TimeWindow window,  // 窗口
                Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
                Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
        ) throws Exception {
            Long itemId = key;
            Long count = aggregateResult.iterator().next();
            collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }