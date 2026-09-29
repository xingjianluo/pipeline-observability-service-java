package com.github.xingjianluo.pipelineobservabilityservicejava.audit;

import java.time.Instant;
import java.util.List;

/**
 * 一次事件处理的判定记录：输入是什么、怎么判的、结果如何，全部留痕。
 */
public record DecisionRecord(
        String pipelineName,
        String runId,
        String eventType,
        Instant eventTime,
        Instant receivedAt,
        String outcome,
        List<String> reasons,
        List<String> firedAlerts,
        Instant decidedAt) {
}
