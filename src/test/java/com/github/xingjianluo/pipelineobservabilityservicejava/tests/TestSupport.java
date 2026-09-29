package com.github.xingjianluo.pipelineobservabilityservicejava.tests;

import com.github.xingjianluo.pipelineobservabilityservicejava.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.EventType;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RunEvent;
import com.github.xingjianluo.pipelineobservabilityservicejava.service.DefaultObservabilityService;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineStatusSnapshot;
import org.slf4j.Logger;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

/** 测试公共构造与事件工厂；集中打印输入事件与判定依据。 */
final class TestSupport {

    private TestSupport() {
    }

    static ObservabilityProperties props(Path dir, Duration freshness, Duration sla,
                                         int failThreshold, Duration suppress) {
        ObservabilityProperties p = new ObservabilityProperties();
        p.setStorageDir(dir.toString());
        p.setDefaultFreshnessThreshold(freshness);
        p.setDefaultSla(sla);
        p.setDefaultFailureAlertThreshold(failThreshold);
        p.setDefaultSuppressWindow(suppress);
        p.setMonitorEnabled(false);
        return p;
    }

    static RunEvent ev(String pipeline, String run, EventType type, Instant t, String detail) {
        return new RunEvent(pipeline, run, type, t, null, detail, t.plusSeconds(1));
    }

    static RunEvent evWithId(String pipeline, String run, EventType type, Instant t,
                             String detail, String eventId) {
        return new RunEvent(pipeline, run, type, t, eventId, detail, t.plusSeconds(1));
    }

    static void logEvent(Logger log, RunEvent e) {
        log.info(">>> 输入事件 pipeline={} run={} type={} eventTime={} detail={}",
                e.pipelineName(), e.runId(), e.type(), e.eventTime(), e.detail());
    }

    static void logSnapshot(Logger log, PipelineStatusSnapshot s) {
        log.info("<<< 判定 snapshot: lastSuccess={} consecutiveFailures={} stale={} slaBreached={} failureThresholdExceeded={} active={}",
                s.lastSuccessAt(), s.consecutiveFailures(), s.stale(), s.slaBreached(),
                s.failureThresholdExceeded(), s.activeAlertTypes());
    }

    static void printDecisions(Logger log, DefaultObservabilityService svc, String pipeline) {
        svc.decisions(pipeline).forEach(d -> log.info(
                "    判定记录 type={} eventTime={} outcome={} reasons={} alerts={}",
                d.eventType(), d.eventTime(), d.outcome(), d.reasons(), d.firedAlerts()));
    }
}
