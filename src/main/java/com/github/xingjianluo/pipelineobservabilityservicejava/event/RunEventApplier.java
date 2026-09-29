package com.github.xingjianluo.pipelineobservabilityservicejava.event;

import com.github.xingjianluo.pipelineobservabilityservicejava.dto.IngestEventRequest;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.EventType;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.PipelineState;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.RunOutcome;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.RunState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 把单条已校验事件幂等地归并到流水线状态。
 *
 * <p>重复、乱序、迟到的处理规则（事件以 eventTime 为准，与到达顺序无关）：</p>
 * <ul>
 *   <li><b>去重</b>：eventId 在整条流水线（跨 run）级别唯一；见过的 eventId 直接忽略，
 *       重复投递不改变任何状态。</li>
 *   <li><b>STARTED</b>：startedAt 取所有 STARTED 事件中最小的 eventTime；迟到的 STARTED
 *       也能修正之前未知的开始时间。</li>
 *   <li><b>RETRIED</b>：attemptCount 取已见事件中的最大重试序号（从 0 开始），乱序到达
 *       不会把计数改小。</li>
 *   <li><b>终态（SUCCESS/FAILED/TIMED_OUT）</b>：先按 eventTime 较晚者为准；eventTime
 *       相同时按优先级 TIMED_OUT &gt; FAILED &gt; SUCCESS 打破平局。迟到的"更晚终态"
 *       可以覆盖早先结论，而迟到的"更早终态"不会推翻新结论。</li>
 * </ul>
 * 由于规则全部基于已见事件集合，以任意顺序、任意重复次数接入，最终状态相同。
 */
@Component
public class RunEventApplier {

    private static final Logger log = LoggerFactory.getLogger(RunEventApplier.class);

    /** 归并结果：是否为重复事件，是否影响了终态，以及该 run 是否已有终态。 */
    public record ApplyOutcome(boolean duplicate, boolean terminalChanged, RunState run) {}

    public ApplyOutcome apply(PipelineState state, IngestEventRequest event) {
        String eventId = event.getEventId();
        if (state.getAcceptedEventIds().contains(eventId)) {
            log.info("[event-apply] DUPLICATE ignored pipeline={} runId={} eventId={} type={} eventTime={}",
                    event.getPipelineName(), event.getRunId(), eventId, event.getEventType(), event.getEventTime());
            return new ApplyOutcome(true, false, state.getRuns().get(event.getRunId()));
        }

        RunState run = state.getRuns().computeIfAbsent(event.getRunId(), id -> {
            RunState created = new RunState();
            created.setPipelineName(state.getPipelineName());
            created.setRunId(id);
            return created;
        });

        EventType type = EventType.valueOf(event.getEventType());
        long eventTime = event.getEventTime();
        boolean terminalChanged = false;

        switch (type) {
            case STARTED -> {
                if (run.getStartedAt() == null || eventTime < run.getStartedAt()) {
                    run.setStartedAt(eventTime);
                }
            }
            case RETRIED -> {
                // 有 eventId 级去重，每接受一条不同的 RETRIED 事件即代表多一次重试尝试
                run.setAttemptCount(run.getAttemptCount() + 1);
            }
            case SUCCESS, FAILED, TIMED_OUT -> {
                EventType previous = run.getTerminalEventType();
                boolean shouldReplace = previous == null || laterOrHigherPriority(type, eventTime, previous, run.getFinishedAt());
                if (shouldReplace) {
                    run.setTerminalEventType(type);
                    run.setFinishedAt(eventTime);
                    run.setOutcome(switch (type) {
                        case SUCCESS -> RunOutcome.SUCCESS;
                        case FAILED -> RunOutcome.FAILED;
                        case TIMED_OUT -> RunOutcome.TIMED_OUT;
                        default -> throw new IllegalStateException("unreachable");
                    });
                    terminalChanged = true;
                }
            }
        }

        state.getAcceptedEventIds().add(eventId);
        run.getAcceptedEventIds().add(eventId);
        log.info("[event-apply] ACCEPTED pipeline={} runId={} eventId={} type={} eventTime={} "
                        + "-> outcome={} startedAt={} finishedAt={} attempts={} terminalChanged={}",
                state.getPipelineName(), run.getRunId(), eventId, type, eventTime,
                run.getOutcome(), run.getStartedAt(), run.getFinishedAt(), run.getAttemptCount() + 1, terminalChanged);
        return new ApplyOutcome(false, terminalChanged, run);
    }

    /** 新终态是否应当覆盖旧终态：时间更晚，或同时间且优先级更高。 */
    private boolean laterOrHigherPriority(EventType newType, long newTime, EventType oldType, Long oldTime) {
        if (oldTime == null) {
            return true;
        }
        if (newTime != oldTime) {
            return newTime > oldTime;
        }
        return terminalRank(newType) > terminalRank(oldType);
    }

    private int terminalRank(EventType type) {
        return switch (type) {
            case SUCCESS -> 1;
            case FAILED -> 2;
            case TIMED_OUT -> 3;
            default -> 0;
        };
    }
}
