package com.lw.myflink.mine;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class BufferingSink
        implements SinkFunction<Tuple2<String, Integer>>,
        CheckpointedFunction {
    // 发送阈值
    private final int threshold;
    // 定义状态，只能是 ListState
    private transient ListState<Tuple2<String, Integer>> checkpointedState;
    // 局部变量，保存最新的数据
    private List<Tuple2<String, Integer>> bufferedElements;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }
    // 实现 SinkFunction 接口，每个元素都会调用一次该函数
    @Override
    public void invoke(Tuple2<String, Integer> value, Context contex) throws Exception {
        // 把数据加入局部变量中
        bufferedElements.add(value);
        // 达到阈值啦！快发送
        if (bufferedElements.size() == threshold) {
            for (Tuple2<String, Integer> element: bufferedElements) {
                // 这里实现发送逻辑
            }
            // 发送完注意清空缓存
            bufferedElements.clear();
        }
    }
    // checkpoint 时会调用 snapshotState() 函数
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 清空 ListState，我们要放入最新的数据啦
        checkpointedState.clear();
        // 把当前局部变量中的所有元素写入到 checkpoint 中
        for (Tuple2<String, Integer> element : bufferedElements) {
            checkpointedState.add(element);
        }
    }
    // 需要处理第一次自定义函数初始化和从之前的 checkpoint 恢复两种情况
    // initializeState 方法接收一个 FunctionInitializationContext 参数，会用来初始化 non-keyed state 的 “容器”。这些容器是一个 ListState， 用于在 checkpoint 时保存 non-keyed state 对象。
    // 就是说我们可以通过 FunctionInitializationContext 获取 ListState 状态
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // StateDescriptor 会包括状态名字、以及状态类型相关信息
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ListStateDescriptor<>(
                        "buffered-elements",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        // context.getOperatorStateStore().getListState(descriptor) 使用 even-split redistribution 算法
        // 我们还可以通过 context.getKeyedStateStore() 获取 keyed state，当然要在 keyedStream 上使用啦！
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        // 需要处理从 checkpoint/savepoint 恢复的情况
        // 通过 isRestored() 方法判断是否从之前的故障中恢复回来，如果该方法返回 true 则表示从故障中进行恢复，会执行接下来的恢复逻辑
        if (context.isRestored()) {
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }
}
