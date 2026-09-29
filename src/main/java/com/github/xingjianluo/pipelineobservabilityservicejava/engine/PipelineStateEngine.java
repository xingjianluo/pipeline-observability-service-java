package com.github.xingjianluo.pipelineobservabilityservicejava.engine;

import com.github.xingjianluo.pipelineobservabilityservicejava.event.RunEvent;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineConfig;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineState;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineStatusSnapshot;

import java.time.Instant;

/**
 * 事件 -> 状态的纯推算引擎。只改内存中的聚合状态，不做持久化与告警，
 * 重复/乱序/迟到事件在这里被吸收。
 */
public interface PipelineStateEngine {

    /**
     * 应用一个事件，返回判定描述（供审计记录 reasons）。
     * 重复事件返回 outcome=DUPLICATE。
     */
    IngestionResult apply(RunEvent event, Instant now);

    /**
     * 基于当前状态与阈值，在 now 时刻评估快照（新鲜度/SLA）。
     */
    PipelineStatusSnapshot evaluate(String pipelineName, PipelineConfig config, Instant now);

    /** 一次接入的判定结果。 */
    record IngestionResult(String outcome, java.util.List<String> reasons) {
    }

    /** 当前内存中的流水线状态（不存在返回 null）。 */
    PipelineState getState(String pipelineName);
}
