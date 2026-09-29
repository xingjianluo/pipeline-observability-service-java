package com.github.xingjianluo.pipelineobservabilityservicejava.engine;

import com.github.xingjianluo.pipelineobservabilityservicejava.event.EventType;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RunEvent;
import com.github.xingjianluo.pipelineobservabilityservicejava.run.RunState;
import com.github.xingjianluo.pipelineobservabilityservicejava.run.RunStatus;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineConfig;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineState;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineStatusSnapshot;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 默认状态推算引擎。
 *
 * <p>关键不变式：</p>
 * <ul>
 *   <li>同内容事件幂等：按 eventId 或 (type,eventTime) 指纹去重；</li>
 *   <li>乱序/迟到不影响结论：每次写入后按事件时间归约、按运行完成时间重算聚合量，
 *       结论只取决于事件集合，与到达顺序无关；</li>
 *   <li>终态可被更早的纠正事件改写，晚于已知终态的事件忽略（迟到的旧消息）。</li>
 * </ul>
 */
@Component
public class DefaultPipelineStateEngine implements PipelineStateEngine {

    private final Map<String, PipelineState> states = new ConcurrentHashMap<>();

    public void loadFrom(Map<String, PipelineState> loaded) {
        states.clear();
        states.putAll(loaded);
    }

    public PipelineState getOrCreate(String pipelineName) {
        return states.computeIfAbsent(pipelineName, name -> {
            PipelineState s = new PipelineState();
            s.setPipelineName(name);
            s.setUpdatedAt(Instant.EPOCH);
            return s;
        });
    }

    public Map<String, PipelineState> all() {
        return states;
    }

    @Override
    public PipelineState getState(String pipelineName) {
        return states.get(pipelineName);
    }

    static String fingerprint(RunEvent e) {
        return e.eventId() != null ? "id:" + e.eventId()
                : "fp:" + e.type() + "|" + e.eventTime();
    }

    @Override
    public IngestionResult apply(RunEvent event, Instant now) {
        PipelineState state = getOrCreate(event.pipelineName());
        RunState run = state.getRuns().get(event.runId());
        List<String> reasons = new ArrayList<>();

        if (run == null) {
            run = new RunState();
            run.setRunId(event.runId());
            state.getRuns().put(event.runId(), run);
            reasons.add("新建运行实例 runId=" + event.runId());
        }

        String fp = fingerprint(event);
        if (run.getSeenEventIds().contains(fp)) {
            reasons.add("重复事件(" + fp + ")，忽略，状态不变");
            return new IngestionResult("DUPLICATE", reasons);
        }

        // 迟到的旧消息：已见过更晚事件，且终态已定时不允许被更晚的非终态事件覆盖。
        if (run.getLastEventTime() != null && event.eventTime().isAfter(run.getLastEventTime()) == false
                && run.isTerminal() && !event.type().isTerminal()) {
            reasons.add("迟到的非终态事件 eventTime=" + event.eventTime()
                    + " 早于已知终态时间 " + run.getEndedAt() + "，忽略");
            return new IngestionResult("IGNORED_STALE", reasons);
        }

        run.getSeenEventIds().add(fp);
        reasons.add("事件 " + event.type() + " @ " + event.eventTime() + " 生效");

        switch (event.type()) {
            case STARTED -> {
                run.setStatus(RunStatus.RUNNING);
                run.setTerminal(false);
                if (run.getStartedAt() == null || event.eventTime().isBefore(run.getStartedAt())) {
                    run.setStartedAt(event.eventTime());
                }
                if (run.getAttempts() == 0) {
                    run.setAttempts(1);
                }
            }
            case RETRYING -> {
                run.setStatus(RunStatus.RUNNING);
                run.setTerminal(false);
                run.setAttempts(run.getAttempts() + 1);
                if (run.getStartedAt() == null) {
                    run.setStartedAt(event.eventTime());
                }
            }
            case SUCCEEDED -> {
                // 更早的成功（乱序纠正）可以覆盖更晚的失败终态；更晚的成功同样覆盖。
                run.setStatus(RunStatus.SUCCEEDED);
                run.setTerminal(true);
                run.setEndedAt(selectEnd(run, event.eventTime()));
            }
            case FAILED -> {
                if (!run.isTerminal() || isCorrection(run, event)) {
                    run.setStatus(RunStatus.FAILED);
                    run.setTerminal(true);
                    run.setEndedAt(selectEnd(run, event.eventTime()));
                }
                run.setLastError(event.detail());
            }
            case TIMED_OUT -> {
                if (!run.isTerminal() || isCorrection(run, event)) {
                    run.setStatus(RunStatus.TIMED_OUT);
                    run.setTerminal(true);
                    run.setEndedAt(selectEnd(run, event.eventTime()));
                }
                run.setLastError(event.detail());
            }
        }

        // 已知最晚事件时间（按业务事件时间，而非接入时间）。
        if (run.getLastEventTime() == null || event.eventTime().isAfter(run.getLastEventTime())) {
            run.setLastEventTime(event.eventTime());
        }
        run.setUpdatedAt(now);
        recompute(state, now);
        return new IngestionResult("APPLIED", reasons);
    }

    /** 更早的终态事件视为对"乱序终态"的纠正，允许改写。 */
    private boolean isCorrection(RunState run, RunEvent event) {
        return run.getEndedAt() != null && event.eventTime().isBefore(run.getEndedAt());
    }

    private Instant selectEnd(RunState run, Instant candidate) {
        if (run.getEndedAt() == null) {
            return candidate;
        }
        // 终态时间取终态事件中最早的一次（保守：以实际结束为准）。
        return candidate.isBefore(run.getEndedAt()) ? candidate : run.getEndedAt();
    }

    /**
     * 基于全部已知运行重算聚合量。聚合只依赖运行集合（插入序保留），
     * 因此乱序写入后结论仍然一致。
     */
    private void recompute(PipelineState state, Instant now) {
        Instant lastSuccess = null;
        long successes = 0;
        long failures = 0;

        // 按完成时间排序，未完成的排在最后，保证连续失败计数只与"完成顺序"有关。
        List<RunState> completed = new ArrayList<>();
        for (RunState r : state.getRuns().values()) {
            if (r.getStatus() == RunStatus.SUCCEEDED) {
                successes++;
                Instant end = r.getEndedAt() != null ? r.getEndedAt() : r.getLastEventTime();
                if (end != null && (lastSuccess == null || end.isAfter(lastSuccess))) {
                    lastSuccess = end;
                }
            } else if (r.getStatus() == RunStatus.FAILED || r.getStatus() == RunStatus.TIMED_OUT) {
                failures++;
            }
            if (r.isTerminal()) {
                completed.add(r);
            }
        }
        completed.sort(Comparator.comparing(RunState::getEndedAt));

        long consecutive = 0;
        for (int i = completed.size() - 1; i >= 0; i--) {
            RunStatus st = completed.get(i).getStatus();
            if (st == RunStatus.SUCCEEDED) {
                break;
            }
            if (st == RunStatus.FAILED || st == RunStatus.TIMED_OUT) {
                consecutive++;
            }
        }

        state.setLastSuccessAt(lastSuccess);
        state.setTotalSuccesses(successes);
        state.setTotalFailures(failures);
        state.setConsecutiveFailures(consecutive);
        state.setVersion(state.getVersion() + 1);
        state.setUpdatedAt(now);
    }

    @Override
    public PipelineStatusSnapshot evaluate(String pipelineName, PipelineConfig config, Instant now) {
        PipelineState state = states.get(pipelineName);
        if (state == null) {
            return new PipelineStatusSnapshot(pipelineName, now, null, 0, 0, 0,
                    false, false, false, false, List.of());
        }

        Duration freshness = config != null ? config.getFreshnessThreshold() : null;
        Duration sla = config != null ? config.getSla() : null;
        int failThreshold = config != null ? config.getFailureAlertThreshold() : Integer.MAX_VALUE;

        boolean stale = freshness != null && state.getLastSuccessAt() != null
                && !state.getLastSuccessAt().plus(freshness).isAfter(now);
        // 从未成功过不算 STALE（没有"最近成功时间"可判断），由连续失败告警覆盖。

        boolean slaBreached = false;
        boolean runningPastSla = false;
        RunState latest = null;
        for (RunState r : state.getRuns().values()) {
            Instant key = r.getLastEventTime() != null ? r.getLastEventTime() : r.getStartedAt();
            if (key == null) {
                continue;
            }
            Instant latestKey = latest != null
                    ? (latest.getLastEventTime() != null ? latest.getLastEventTime() : latest.getStartedAt())
                    : null;
            if (latest == null || key.isAfter(latestKey)) {
                latest = r;
            }
        }
        if (sla != null && latest != null && latest.getStartedAt() != null) {
            Instant deadline = latest.getStartedAt().plus(sla);
            if (latest.isTerminal() && latest.getEndedAt() != null) {
                // 延迟完成：结束晚于 SLA 截止。
                slaBreached = latest.getEndedAt().isAfter(deadline);
            } else if (latest.getStatus() == RunStatus.RUNNING) {
                // 运行中超时必须有"近期还在跑"的证据：不仅开始时间过了截止，
                // 最近一次活动（最后事件时间）也过了截止，避免历史事件在当前时刻被误判。
                runningPastSla = now.isAfter(deadline)
                        && latest.getLastEventTime() != null
                        && !latest.getLastEventTime().plus(sla).isAfter(now);
                slaBreached = runningPastSla;
            }
        }

        boolean failureThresholdExceeded = failThreshold > 0
                && state.getConsecutiveFailures() >= failThreshold;

        return new PipelineStatusSnapshot(pipelineName, now, state.getLastSuccessAt(),
                state.getConsecutiveFailures(), state.getTotalSuccesses(), state.getTotalFailures(),
                stale, slaBreached, runningPastSla, failureThresholdExceeded, List.of());
    }
}
