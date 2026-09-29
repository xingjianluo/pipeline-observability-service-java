package com.github.xingjianluo.pipelineobservabilityservicejava.service;

import com.github.xingjianluo.pipelineobservabilityservicejava.alert.AlertNotification;
import com.github.xingjianluo.pipelineobservabilityservicejava.dto.IngestEventRequest;
import com.github.xingjianluo.pipelineobservabilityservicejava.dto.RejectedEvent;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.EventIngestResult;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.EventValidator;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RunEventApplier;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.ValidationResult;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.PipelineDerivedStatus;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.PipelineState;
import com.github.xingjianluo.pipelineobservabilityservicejava.store.PipelineStateStore;
import com.github.xingjianluo.pipelineobservabilityservicejava.support.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 门面服务：原始事件 → 校验 →（拒绝则按原因留痕 / 通过则在 pipeline 锁内归并）
 * → 推算状态 → 评估告警。事件接入与告警状态的更新在同一把 pipeline 锁内原子完成。
 */
@Service
public class PipelineObservabilityService {

    private static final Logger log = LoggerFactory.getLogger(PipelineObservabilityService.class);

    private final EventValidator validator;
    private final RunEventApplier applier;
    private final PipelineStateStore store;
    private final StatusCalculator statusCalculator;
    private final AlertEvaluator alertEvaluator;
    private final Clock clock;

    public PipelineObservabilityService(EventValidator validator,
                                        RunEventApplier applier,
                                        PipelineStateStore store,
                                        StatusCalculator statusCalculator,
                                        AlertEvaluator alertEvaluator,
                                        Clock clock) {
        this.validator = validator;
        this.applier = applier;
        this.store = store;
        this.statusCalculator = statusCalculator;
        this.alertEvaluator = alertEvaluator;
        this.clock = clock;
    }

    /** 接入一条原始 JSON 事件。 */
    public EventIngestResult ingestRaw(String rawJson) {
        log.info("[ingest] IN raw={}", rawJson);
        ValidationResult result = validator.validateRaw(rawJson);
        if (!result.isValid()) {
            reject(result.request(), result.reason(), rawJson);
            return EventIngestResult.rejected(result.reason());
        }
        return ingest(result.request());
    }

    /** 接入一条已校验/已解析事件（编程式入口，仍会做完整校验）。 */
    public EventIngestResult ingest(IngestEventRequest request) {
        log.info("[ingest] IN eventId={} pipeline={} runId={} type={} eventTime={}",
                request.getEventId(), request.getPipelineName(), request.getRunId(),
                request.getEventType(), request.getEventTime());

        String pipelineName = request.getPipelineName();
        // 整个"去重判断 + 归并 + 告警评估"在 store 的同一把 pipeline 锁内一次完成，并发写不丢更新
        final boolean[] duplicateHolder = {false};
        final List<AlertNotification>[] alertsHolder = new List[]{List.of()};

        store.compute(pipelineName, state -> {
            if (state.getAcceptedEventIds().contains(request.getEventId())) {
                duplicateHolder[0] = true;
                return state;
            }
            RunEventApplier.ApplyOutcome outcome = applier.apply(state, request);
            if (outcome.terminalChanged()) {
                alertsHolder[0] = alertEvaluator.evaluateAfterEvent(state, request.getRunId());
            }
            return state;
        });

        if (duplicateHolder[0]) {
            log.info("[ingest] DUPLICATE eventId={} pipeline={} —— 幂等忽略", request.getEventId(), pipelineName);
            return EventIngestResult.duplicate(request.getEventId());
        }

        List<AlertNotification> alerts = alertsHolder[0];

        PipelineDerivedStatus status = statusOf(pipelineName);
        log.info("[ingest] ACCEPTED eventId={} pipeline={} -> lastSuccessAt={} consecutiveFailures={} "
                        + "freshnessExceeded={} slaExceededOnLastRun={} alertsFired={}",
                request.getEventId(), pipelineName, status.lastSuccessAt(), status.consecutiveFailures(),
                status.freshnessExceeded(), status.slaExceededOnLastRun(), alerts.size());
        return EventIngestResult.accepted();
    }

    private void reject(IngestEventRequest partial,
                        com.github.xingjianluo.pipelineobservabilityservicejava.model.RejectReason reason,
                        String rawJson) {
        RejectedEvent rejected = new RejectedEvent();
        if (partial != null) {
            rejected.setEventId(partial.getEventId());
            rejected.setPipelineName(partial.getPipelineName());
            rejected.setRunId(partial.getRunId());
            rejected.setRawEventType(partial.getEventType());
            rejected.setRawEventTime(partial.getEventTime());
        }
        rejected.setReason(reason.name());
        rejected.setRawPayload(rawJson);
        rejected.setRejectedAt(clock.nowMillis());
        store.appendRejected(rejected);
        log.warn("[ingest] REJECTED eventId={} pipeline={} reason={} raw={}",
                rejected.getEventId(), rejected.getPipelineName(), reason, rawJson);
    }

    public PipelineDerivedStatus statusOf(String pipelineName) {
        // 在 pipeline 锁内派生不可变视图，避免与并发更新同时遍历可变状态
        return store.readWithLock(pipelineName, statusCalculator::derive);
    }

    public List<PipelineDerivedStatus> allStatuses() {
        // 逐条在各自锁内取快照，避免遍历活动态时被并发写改动
        return store.findAllNames().stream()
                .map(name -> store.readWithLock(name, statusCalculator::derive))
                .toList();
    }

    public List<RejectedEvent> rejectedEvents() {
        return store.findRejected();
    }
}
