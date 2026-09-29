package com.github.xingjianluo.pipelineobservabilityservicejava.model;

/** 由 PipelineState + 阈值 + 当前时间派生出的对外状态视图（不落盘）。 */
public record PipelineDerivedStatus(
        String pipelineName,
        Long lastSuccessAt,
        Long lastRunFinishedAt,
        int consecutiveFailures,
        long freshnessThresholdMs,
        long slaThresholdMs,
        int consecutiveFailureThreshold,
        boolean freshnessExceeded,
        boolean slaExceededOnLastRun,
        long millisSinceLastSuccess,
        Long earliestActivityAt
) {}
