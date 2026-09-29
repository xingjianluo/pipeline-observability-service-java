package com.github.xingjianluo.pipelineobservabilityservicejava.service;

import com.github.xingjianluo.pipelineobservabilityservicejava.alert.AlertNotification;
import com.github.xingjianluo.pipelineobservabilityservicejava.alert.AlertSink;
import com.github.xingjianluo.pipelineobservabilityservicejava.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.AlertState;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.AlertType;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.PipelineDerivedStatus;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.PipelineState;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.RunState;
import com.github.xingjianluo.pipelineobservabilityservicejava.support.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 告警评估。三类问题严格区分，不合并为笼统失败：
 * <ol>
 *   <li>{@link AlertType#CONSECUTIVE_FAILURES}：连续失败/超时次数达到阈值；一次成功即恢复；</li>
 *   <li>{@link AlertType#DELAYED_COMPLETION}：某次运行 startedAt→finishedAt 超过 SLA；按 run
 *       判定与抑制，同一 run 只提醒一次；</li>
 *   <li>{@link AlertType#STALE_NO_SUCCESS}：从未成功或距最近成功超过新鲜度阈值；重新成功即恢复。</li>
 * </ol>
 *
 * <p>抑制窗口：某作用域内同一问题处于 active 时，在 suppressionWindow 内不重复提醒；
 * 超过窗口且问题仍在持续会再提醒一次（心跳式提醒）。问题恢复时 active 翻为 false，
 * 下一次再触发可重新提醒（fireCount 累计，但会作为一个新的问题周期发出通知）。</p>
 *
 * <p>告警状态存于 PipelineState 中一起落盘，重启后抑制与恢复结论不丢失。</p>
 */
@Component
public class AlertEvaluator {

    private static final Logger log = LoggerFactory.getLogger(AlertEvaluator.class);

    private final ObservabilityProperties properties;
    private final StatusCalculator statusCalculator;
    private final Clock clock;
    private final AlertSink alertSink;

    public AlertEvaluator(ObservabilityProperties properties,
                          StatusCalculator statusCalculator,
                          Clock clock,
                          AlertSink alertSink) {
        this.properties = properties;
        this.statusCalculator = statusCalculator;
        this.clock = clock;
        this.alertSink = alertSink;
    }

    /** 事件驱动评估：在一次终态事件被接受之后调用。 */
    public List<AlertNotification> evaluateAfterEvent(PipelineState state, String changedRunId) {
        long now = clock.nowMillis();
        PipelineDerivedStatus derived = statusCalculator.derive(state);
        List<AlertNotification> fired = new ArrayList<>();

        evaluateConsecutiveFailures(state, derived, now, fired);
        evaluateDelayedCompletion(state, changedRunId, now, fired);
        evaluateStale(state, derived, now, fired);

        fired.forEach(alertSink::send);
        return fired;
    }

    /** 时间驱动评估：定时巡检调用，捕捉"长时间没有成功"这类随时间流逝产生的问题。 */
    public List<AlertNotification> evaluateByTime(PipelineState state) {
        long now = clock.nowMillis();
        PipelineDerivedStatus derived = statusCalculator.derive(state);
        List<AlertNotification> fired = new ArrayList<>();
        evaluateStale(state, derived, now, fired);
        fired.forEach(alertSink::send);
        return fired;
    }

    private void evaluateConsecutiveFailures(PipelineState state, PipelineDerivedStatus d, long now,
                                             List<AlertNotification> fired) {
        String key = state.getPipelineName();
        boolean problem = d.consecutiveFailures() >= d.consecutiveFailureThreshold();
        if (problem) {
            maybeFire(state, AlertType.CONSECUTIVE_FAILURES, key,
                    "连续失败 " + d.consecutiveFailures() + " 次，达到阈值 " + d.consecutiveFailureThreshold()
                            + "（最近终态时间 " + d.lastRunFinishedAt() + "）",
                    now, fired, null);
        } else {
            markRecovered(state, AlertType.CONSECUTIVE_FAILURES, key, now,
                    "连续失败计数回落到 " + d.consecutiveFailures() + "（有成功运行或尚未达阈值）");
        }
    }

    private void evaluateDelayedCompletion(PipelineState state, String changedRunId, long now,
                                           List<AlertNotification> fired) {
        if (changedRunId == null) {
            return;
        }
        RunState run = state.getRuns().get(changedRunId);
        if (run == null || run.getStartedAt() == null || run.getFinishedAt() == null) {
            return;
        }
        long duration = run.getFinishedAt() - run.getStartedAt();
        long sla = properties.getSlaThresholdMs();
        if (duration > sla) {
            maybeFire(state, AlertType.DELAYED_COMPLETION, changedRunId,
                    "运行 " + changedRunId + " 耗时 " + duration + "ms 超过 SLA " + sla
                            + "ms，结局=" + run.getOutcome(),
                    now, fired, changedRunId);
        }
        // 延迟是单次 run 的既成事实，不存在"恢复"；active 记录保留，供事后查询
    }

    private void evaluateStale(PipelineState state, PipelineDerivedStatus d, long now,
                               List<AlertNotification> fired) {
        String key = state.getPipelineName();
        if (d.freshnessExceeded()) {
            String detail = d.lastSuccessAt() == null
                    ? "从未有过成功运行，最早活动距今 " + (d.earliestActivityAt() == null ? 0 : now - d.earliestActivityAt())
                            + "ms，超过新鲜度阈值 " + d.freshnessThresholdMs() + "ms"
                    : "距最近成功已 " + (now - d.lastSuccessAt()) + "ms，超过新鲜度阈值 " + d.freshnessThresholdMs() + "ms";
            maybeFire(state, AlertType.STALE_NO_SUCCESS, key, detail, now, fired, null);
        } else {
            // 只有曾经成功过（新鲜度以 lastSuccessAt 计）才可能存在"从 stale 恢复"；
            // 从未成功但尚未超阈值属于"还没到点"，本就没有告警，无需标记恢复
            if (d.lastSuccessAt() == null) {
                return;
            }
            markRecovered(state, AlertType.STALE_NO_SUCCESS, key, now,
                    "新鲜度恢复：最近成功距今 " + (now - d.lastSuccessAt()) + "ms，在阈值 "
                            + d.freshnessThresholdMs() + "ms 内");
        }
    }

    /** 按抑制/重触发规则发出告警，并更新告警状态。 */
    private void maybeFire(PipelineState state, AlertType type, String scopeKey, String message,
                           long now, List<AlertNotification> fired, String runId) {
        Map<AlertType, AlertState> all = state.getAlerts();
        AlertState alert = all.get(type);
        boolean isNewScope = alert == null || !scopeKey.equals(alert.getScopeKey());

        if (alert == null) {
            alert = new AlertState();
            alert.setType(type);
            alert.setScopeKey(scopeKey);
            all.put(type, alert);
        }

        boolean shouldFire;
        String why;
        if (isNewScope) {
            // 作用域变化（例如 DELAYED_COMPLETION 换了一个 run）视为新问题
            shouldFire = true;
            why = "新作用域 " + scopeKey + " 首次触发";
            alert.setActive(true);
            alert.setFirstFiredAt(now);
            alert.setFireCount(1);
        } else if (!alert.isActive()) {
            // 问题曾恢复（或从未触发），重新触发
            shouldFire = true;
            why = "问题恢复后重新触发";
            alert.setActive(true);
            alert.setFirstFiredAt(now);
            alert.setRecoveredAt(null);
            alert.setFireCount(alert.getFireCount() + 1);
        } else if (now - alert.getLastFiredAt() >= properties.getAlertSuppressionWindowMs()) {
            // 抑制窗口外的心跳式重复提醒
            shouldFire = true;
            why = "抑制窗口 " + properties.getAlertSuppressionWindowMs() + "ms 已过，问题仍持续";
            alert.setFireCount(alert.getFireCount() + 1);
        } else {
            shouldFire = false;
            why = "抑制窗口内（距上次提醒 " + (now - alert.getLastFiredAt()) + "ms），不重复提醒";
        }

        if (shouldFire) {
            alert.setLastFiredAt(now);
            AlertNotification notification = new AlertNotification(
                    type.name(), state.getPipelineName(), runId, now, message);
            fired.add(notification);
            log.warn("[alert] FIRE type={} pipeline={} scope={} fireCount={} basis={} | {}",
                    type, state.getPipelineName(), scopeKey, alert.getFireCount(), why, message);
        } else {
            log.info("[alert] SUPPRESS type={} pipeline={} scope={} basis={}",
                    type, state.getPipelineName(), scopeKey, why);
        }
    }

    private void markRecovered(PipelineState state, AlertType type, String scopeKey, long now, String why) {
        AlertState alert = state.getAlerts().get(type);
        if (alert != null && alert.isActive() && scopeKey.equals(alert.getScopeKey())) {
            alert.setActive(false);
            alert.setRecoveredAt(now);
            log.info("[alert] RECOVER type={} pipeline={} scope={} basis={}",
                    type, state.getPipelineName(), scopeKey, why);
        }
    }
}
