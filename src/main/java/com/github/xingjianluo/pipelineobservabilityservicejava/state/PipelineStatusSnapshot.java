package com.github.xingjianluo.pipelineobservabilityservicejava.state;

import java.time.Instant;
import java.util.List;

/**
 * 某时刻对流水线状态评估的不可变结果：状态量 + 新鲜度/SLA 判定 + 活跃告警。
 */
public record PipelineStatusSnapshot(
        String pipelineName,
        Instant evaluatedAt,
        Instant lastSuccessAt,
        long consecutiveFailures,
        long totalSuccesses,
        long totalFailures,
        boolean stale,
        boolean slaBreached,
        /** 存在运行中实例，且其最近活动时间也已过 SLA 截止（有"超时未结束"的近期证据）。 */
        boolean runningPastSla,
        boolean failureThresholdExceeded,
        List<String> activeAlertTypes) {
}
