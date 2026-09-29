package com.github.xingjianluo.pipelineobservabilityservicejava.service;

import com.github.xingjianluo.pipelineobservabilityservicejava.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.PipelineDerivedStatus;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.PipelineState;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.RunOutcome;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.RunState;
import com.github.xingjianluo.pipelineobservabilityservicejava.support.Clock;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;

/**
 * 纯函数式状态推算。
 *
 * <p>所有结论只依赖 {@link PipelineState}（已落盘的事件归并结果）+ 阈值 + 当前时间，
 * 不依赖事件到达顺序与进程内可变计数，因此重启后重新推算的结论与重启前完全一致。</p>
 *
 * <p>判定规则：</p>
 * <ul>
 *   <li>lastSuccessAt：所有成功终态 run 的 finishedAt 最大值；</li>
 *   <li>consecutiveFailures：按 finishedAt 倒序排列终态 run，从最近一次往前数，
 *       连续为 FAILED/TIMED_OUT 的个数（SUCCESS 即中断）；</li>
 *   <li>freshnessExceeded：有成功时，now - lastSuccessAt &gt; freshnessThresholdMs；
 *       从未成功时，以"最早活动时间"（最早 STARTED 或终态事件的最小时间）为基准，
 *       now - earliestActivityAt &gt; 阈值才算 stale——避免刚启动、第一次失败就误报"长时间没成功"；
 *       完全没有事件的流水线不算 stale；</li>
 *   <li>slaExceededOnLastRun：最近一次终态 run 若有 startedAt，
 *       其 finishedAt - startedAt &gt; slaThresholdMs（成功/失败均可算延迟完成）。</li>
 * </ul>
 */
@Component
public class StatusCalculator {

    private final ObservabilityProperties properties;
    private final Clock clock;

    public StatusCalculator(ObservabilityProperties properties, Clock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    public PipelineDerivedStatus derive(PipelineState state) {
        long now = clock.nowMillis();
        String name = state == null ? null : state.getPipelineName();

        List<RunState> terminals = state == null ? List.of()
                : state.getRuns().values().stream()
                .filter(r -> r.getOutcome() != null && r.getFinishedAt() != null)
                .sorted(Comparator.comparingLong(RunState::getFinishedAt).reversed())
                .toList();

        Long lastSuccessAt = terminals.stream()
                .filter(r -> r.getOutcome() == RunOutcome.SUCCESS)
                .map(RunState::getFinishedAt)
                .max(Long::compare)
                .orElse(null);

        int consecutiveFailures = 0;
        for (RunState run : terminals) {
            if (run.getOutcome() == RunOutcome.SUCCESS) {
                break;
            }
            consecutiveFailures++;
        }

        Long lastRunFinishedAt = terminals.isEmpty() ? null : terminals.get(0).getFinishedAt();

        long freshness = properties.getFreshnessThresholdMs();
        long sla = properties.getSlaThresholdMs();
        int failThreshold = properties.getConsecutiveFailureThreshold();

        long millisSinceLastSuccess = lastSuccessAt == null ? Long.MAX_VALUE : Math.max(0L, now - lastSuccessAt);

        // 从未成功时以最早活动时间为基准；完全没有事件的流水线不判 stale
        Long earliestActivityAt = state == null ? null : state.getRuns().values().stream()
                .flatMap(r -> java.util.stream.Stream.of(r.getStartedAt(), r.getFinishedAt()))
                .filter(java.util.Objects::nonNull)
                .min(Long::compare)
                .orElse(null);

        boolean freshnessExceeded;
        if (lastSuccessAt != null) {
            freshnessExceeded = millisSinceLastSuccess > freshness;
        } else {
            freshnessExceeded = earliestActivityAt != null && now - earliestActivityAt > freshness;
        }

        boolean slaExceededOnLastRun = false;
        if (!terminals.isEmpty()) {
            RunState last = terminals.get(0);
            if (last.getStartedAt() != null && last.getFinishedAt() != null) {
                slaExceededOnLastRun = last.getFinishedAt() - last.getStartedAt() > sla;
            }
        }

        return new PipelineDerivedStatus(
                name,
                lastSuccessAt,
                lastRunFinishedAt,
                consecutiveFailures,
                freshness,
                sla,
                failThreshold,
                freshnessExceeded,
                slaExceededOnLastRun,
                millisSinceLastSuccess,
                earliestActivityAt
        );
    }
}
