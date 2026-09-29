package com.github.xingjianluo.pipelineobservabilityservicejava.store;

import com.github.xingjianluo.pipelineobservabilityservicejava.dto.RejectedEvent;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.PipelineState;

import java.util.List;

/**
 * 流水线状态存储。
 * 实现需保证：同一 pipeline 的并发更新互斥（不丢更新、不覆盖），
 * 状态落盘以便重启后恢复，重启前后结论一致。
 */
public interface PipelineStateStore {

    /** 不存在则新建，再交给更新函数；整个读-改-写对该 pipeline 串行化。 */
    PipelineState compute(String pipelineName, java.util.function.Function<PipelineState, PipelineState> updater);

    /**
     * 在该 pipeline 的锁内执行只读函数。返回值由调用方自行构造为不可变对象，
     * 保证读期间状态不会被并发更新改写（避免锁外遍历可变状态导致 ConcurrentModification）。
     */
    <T> T readWithLock(String pipelineName, java.util.function.Function<PipelineState, T> reader);

    PipelineState get(String pipelineName);

    List<PipelineState> findAll();

    /** 当前已知的全部流水线名（快照），供逐条加锁读取，避免遍历活动态。 */
    List<String> findAllNames();

    void appendRejected(RejectedEvent rejectedEvent);

    List<RejectedEvent> findRejected();
}
