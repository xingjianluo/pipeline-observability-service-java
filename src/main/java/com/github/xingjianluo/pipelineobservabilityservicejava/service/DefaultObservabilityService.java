package com.github.xingjianluo.pipelineobservabilityservicejava.service;

import com.github.xingjianluo.pipelineobservabilityservicejava.alert.Alert;
import com.github.xingjianluo.pipelineobservabilityservicejava.alert.AlertService;
import com.github.xingjianluo.pipelineobservabilityservicejava.audit.AuditLog;
import com.github.xingjianluo.pipelineobservabilityservicejava.audit.DecisionRecord;
import com.github.xingjianluo.pipelineobservabilityservicejava.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityservicejava.engine.DefaultPipelineStateEngine;
import com.github.xingjianluo.pipelineobservabilityservicejava.engine.PipelineStateEngine;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RejectReason;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RejectedEventException;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RunEvent;
import com.github.xingjianluo.pipelineobservabilityservicejava.ingest.EventParser;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineConfig;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineState;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineStatusSnapshot;
import com.github.xingjianluo.pipelineobservabilityservicejava.store.StateStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 默认门面实现。
 *
 * <p>每条流水线一把锁（interned），并发写同一条流水线被串行化，
 * 不同流水线互不阻塞；每次推算后立即落盘，重启不丢结论。</p>
 */
@Service
public class DefaultObservabilityService implements ObservabilityService {

    private static final Logger log = LoggerFactory.getLogger(DefaultObservabilityService.class);

    private final EventParser parser;
    private final DefaultPipelineStateEngine engine;
    private final AlertService alertService;
    private final StateStore store;
    private final AuditLog auditLog;
    private final ObservabilityProperties props;

    /** 每流水线一把锁，锁住"推算+告警+落盘"整个临界区。 */
    private final Map<String, Object> pipelineLocks = new ConcurrentHashMap<>();

    public DefaultObservabilityService(EventParser parser,
                                       DefaultPipelineStateEngine engine,
                                       AlertService alertService,
                                       StateStore store,
                                       AuditLog auditLog,
                                       ObservabilityProperties props) {
        this.parser = parser;
        this.engine = engine;
        this.alertService = alertService;
        this.store = store;
        this.auditLog = auditLog;
        this.props = props;
    }

    @PostConstruct
    public void restore() {
        engine.loadFrom(store.loadPipelineStates());
        log.info("服务启动，恢复流水线状态 {} 条", engine.all().size());
    }

    private Object lockFor(String pipelineName) {
        return pipelineLocks.computeIfAbsent(pipelineName, k -> new Object());
    }

    @Override
    public DecisionRecord ingest(String rawPayload, Instant now) {
        RunEvent event;
        try {
            event = parser.parse(rawPayload, now);
        } catch (RejectedEventException ex) {
            // 被拒绝的事件也要留痕：按原因区分，绝不悄悄丢弃。
            DecisionRecord rec = new DecisionRecord(
                    "-", "-", "-", null, now,
                    "REJECTED:" + ex.getReason().name(),
                    List.of(ex.getMessage()), List.of(), now);
            auditLog.append(rec);
            throw ex;
        }
        return ingest(event, now);
    }

    @Override
    public DecisionRecord ingest(RunEvent event, Instant ignored) {
        // 事件驱动的评估以"接入时刻"为现在：历史 eventTime 的事件不应被真实时钟误判超时。
        Instant now = event.receivedAt();
        log.info("接入事件 pipeline={} run={} type={} eventTime={} receivedAt={}",
                event.pipelineName(), event.runId(), event.type(), event.eventTime(), now);
        synchronized (lockFor(event.pipelineName())) {
            PipelineStateEngine.IngestionResult result = engine.apply(event, now);
            PipelineState state = engine.getState(event.pipelineName());
            store.savePipelineState(state);

            PipelineConfig config = resolveConfig(event.pipelineName());
            PipelineStatusSnapshot snapshot = engine.evaluate(event.pipelineName(), config, now);
            List<Alert> fired = alertService.evaluate(snapshot, config, now);
            List<String> alerts = fired.stream().map(a -> a.type().name()).toList();

            List<String> reasons = new ArrayList<>(result.reasons());
            reasons.add("聚合: lastSuccess=" + snapshot.lastSuccessAt()
                    + " consecutiveFailures=" + snapshot.consecutiveFailures()
                    + " stale=" + snapshot.stale()
                    + " slaBreached=" + snapshot.slaBreached()
                    + " failureThresholdExceeded=" + snapshot.failureThresholdExceeded());

            DecisionRecord rec = new DecisionRecord(
                    event.pipelineName(), event.runId(), event.type().name(),
                    event.eventTime(), event.receivedAt(),
                    result.outcome(), reasons, alerts, now);
            auditLog.append(rec);
            log.info("判定结果: {} | reasons={} | alerts={}", result.outcome(), reasons, alerts);
            return rec;
        }
    }

    @Override
    public PipelineStatusSnapshot status(String pipelineName, Instant now) {
        synchronized (lockFor(pipelineName)) {
            PipelineConfig config = resolveConfig(pipelineName);
            PipelineStatusSnapshot snap = engine.evaluate(pipelineName, config, now);
            List<String> active = alertService.activeAlerts(pipelineName).stream()
                    .map(Enum::name).toList();
            return new PipelineStatusSnapshot(snap.pipelineName(), snap.evaluatedAt(),
                    snap.lastSuccessAt(), snap.consecutiveFailures(), snap.totalSuccesses(),
                    snap.totalFailures(), snap.stale(), snap.slaBreached(), snap.runningPastSla(),
                    snap.failureThresholdExceeded(), active);
        }
    }

    @Override
    public void evaluateTimeBasedAlerts(Instant now) {
        // 对所有已知流水线做一次"只有时间流逝"的评估，覆盖迟到巡检场景。
        for (String pipeline : new ArrayList<>(engine.all().keySet())) {
            synchronized (lockFor(pipeline)) {
                PipelineConfig config = resolveConfig(pipeline);
                PipelineStatusSnapshot snap = engine.evaluate(pipeline, config, now);
                alertService.evaluate(snap, config, now, true);
            }
        }
    }

    @Override
    public PipelineConfig configure(PipelineConfig config) {
        PipelineConfig merged = config.getPipelineName() != null
                ? mergeDefaults(config) : config;
        store.saveConfig(merged);
        return merged;
    }

    private PipelineConfig resolveConfig(String pipelineName) {
        PipelineConfig stored = store.loadConfigs().get(pipelineName);
        if (stored != null) {
            return stored;
        }
        PipelineConfig c = new PipelineConfig();
        c.setPipelineName(pipelineName);
        c.setFreshnessThreshold(props.getDefaultFreshnessThreshold());
        c.setSla(props.getDefaultSla());
        c.setFailureAlertThreshold(props.getDefaultFailureAlertThreshold());
        c.setAlertSuppressWindow(props.getDefaultSuppressWindow());
        return c;
    }

    private PipelineConfig mergeDefaults(PipelineConfig c) {
        if (c.getFreshnessThreshold() == null) {
            c.setFreshnessThreshold(props.getDefaultFreshnessThreshold());
        }
        if (c.getSla() == null) {
            c.setSla(props.getDefaultSla());
        }
        if (c.getFailureAlertThreshold() == 0) {
            c.setFailureAlertThreshold(props.getDefaultFailureAlertThreshold());
        }
        if (c.getAlertSuppressWindow() == null) {
            c.setAlertSuppressWindow(props.getDefaultSuppressWindow());
        }
        return c;
    }

    @Override
    public List<DecisionRecord> decisions(String pipelineName) {
        return auditLog.findByPipeline(pipelineName);
    }

    @Override
    public List<Alert> alerts(String pipelineName) {
        return alertService.history(pipelineName);
    }

    /** 拒绝原因枚举暴露给 web 层做默认消息（便捷）。 */
    public static String rejectMessage(RejectReason reason) {
        return reason.getDefaultMessage();
    }
}
