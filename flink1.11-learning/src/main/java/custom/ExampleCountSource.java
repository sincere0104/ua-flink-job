package custom;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @ClassName ExampleCountSource
 * @Description TODO 自定义source
 * @Author zby
 * @Date 2021-12-21 16:37
 * @Version 1.0
 **/
public class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
    private long count = 0L;
    /**
     * 使用一个volatile类型变量控制run方法内循环的运行
     */
    private volatile boolean isRunning = true;
    /**
     * 保存数据源状态的变量
     */
    private transient ListState<Long> checkpointedCount;

    @Override
    public void run(SourceContext<Long> ctx) {
        while (isRunning && count < 1000) {
            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            // 此处必须要加锁，防止在checkpoint过程中，仍然发送数据
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(count);
                count++;
            }
        }
    }

    @Override
    public void cancel() {
        // 设置isRunning为false，终止run方法内循环的运行
        isRunning = false;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 获取存储状态
        this.checkpointedCount = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("count", Long.class));
        // 如果数据源是从失败中恢复，则读取count的值，恢复数据源count状态
        if (context.isRestored()) {
            for (Long count : this.checkpointedCount.get()) {
                this.count = count;
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 保存数据到状态变量
        this.checkpointedCount.clear();
        this.checkpointedCount.add(count);
    }
}
