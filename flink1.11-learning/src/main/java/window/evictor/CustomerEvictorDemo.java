package window.evictor;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

/**
 * @ClassName CustomerEvictorDemo
 * @Description TODO 自定义驱逐器 可以自己定义在窗口触发计算前，剔除什么数据，避免其参与计算
 * @Author zby
 * @Date 2021-12-13 13:09
 * @Version 1.0
 **/
public class CustomerEvictorDemo<W extends Window> implements Evictor<Object, W> {

    /**
     *
     * @param elements
     * @param size
     * @param window
     * @param evictorContext
     */
    @Override
    public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext evictorContext) {

    }

    /**
     *
     * @param elements
     * @param size
     * @param window
     * @param evictorContext
     */
    @Override
    public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext evictorContext) {

    }
}
