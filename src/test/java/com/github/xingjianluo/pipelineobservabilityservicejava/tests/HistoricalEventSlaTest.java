package com.github.xingjianluo.pipelineobservabilityservicejava.tests;

import com.github.xingjianluo.pipelineobservabilityservicejava.alert.AlertType;
import com.github.xingjianluo.pipelineobservabilityservicejava.alert.DefaultAlertService;
import com.github.xingjianluo.pipelineobservabilityservicejava.audit.FileAuditLog;
import com.github.xingjianluo.pipelineobservabilityservicejava.audit.FileAuditLog;
import com.github.xingjianluo.pipelineobservabilityservicejava.engine.DefaultPipelineStateEngine;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.EventType;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RunEvent;
import com.github.xingjianluo.pipelineobservabilityservicejava.ingest.JacksonEventParser;
import com.github.xingjianluo.pipelineobservabilityservicejava.service.DefaultObservabilityService;
import com.github.xingjianluo.pipelineobservabilityservicejava.store.FileStateStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** 历史 eventTime 的事件：接入时按接入时刻评估，不能被真实时钟误判为运行超时。 */
class HistoricalEventSlaTest {

    private static final Logger log = LoggerFactory.getLogger(HistoricalEventSlaTest.class);

    @Test
    void historicalStartedEventIsNotFlaggedAsRunningPastSlaAtIngestion(@TempDir Path dir) {
        var props = TestSupport.props(dir, Duration.ofMinutes(30), Duration.ofSeconds(10),
                3, Duration.ofMinutes(15));
        FileStateStore store = new FileStateStore(props);
        DefaultPipelineStateEngine engine = new DefaultPipelineStateEngine();
        engine.loadFrom(store.loadPipelineStates());
        DefaultAlertService alerts = new DefaultAlertService(store);
        alerts.load();
        var svc = new DefaultObservabilityService(new JacksonEventParser(), engine, alerts,
                store, new FileAuditLog(store), props);

        // 事件发生在很久以前，receivedAt = eventTime + 1s。
        Instant t0 = Instant.parse("2020-01-01T00:00:00Z");
        RunEvent started = new RunEvent("old-pipe", "r1", EventType.STARTED, t0, null, null,
                t0.plusSeconds(1));
        TestSupport.logEvent(log, started);
        svc.ingest(started, started.receivedAt());

        long slaAlerts = svc.alerts("old-pipe").stream()
                .filter(a -> a.type() == AlertType.SLA_BREACHED).count();
        var snap = svc.status("old-pipe", t0.plusSeconds(1));
        TestSupport.logSnapshot(log, snap);
        assertFalse(snap.slaBreached(), "接入仅 1 秒的运行不应被判定超过 10s SLA");
        assertEquals(0, slaAlerts, "历史 STARTED 事件不能产生误报告警");
        log.info("无 SLA 误报，验证通过");
    }

    @Test
    void historicalSuccessEventDoesNotRaiseStaleAlertAtIngestion(@TempDir Path dir) {
        var props = TestSupport.props(dir, Duration.ofSeconds(30), Duration.ofHours(1),
                3, Duration.ofMinutes(15));
        FileStateStore store = new FileStateStore(props);
        DefaultPipelineStateEngine engine = new DefaultPipelineStateEngine();
        engine.loadFrom(store.loadPipelineStates());
        DefaultAlertService alerts = new DefaultAlertService(store);
        alerts.load();
        var svc = new DefaultObservabilityService(new JacksonEventParser(), engine, alerts,
                store, new FileAuditLog(store), props);

        Instant t0 = Instant.parse("2020-01-01T00:00:00Z");
        RunEvent ok = new RunEvent("old-stale", "r2", EventType.SUCCEEDED, t0, null, null,
                t0.plusSeconds(1));
        TestSupport.logEvent(log, ok);
        svc.ingest(ok, ok.receivedAt());

        long staleAlerts = svc.alerts("old-stale").stream()
                .filter(a -> a.type() == AlertType.STALE_NO_SUCCESS).count();
        assertEquals(0, staleAlerts, "历史成功事件接入时不能因真实时钟已远晚于它而误报 stale");
        log.info("接入路径无 stale 误报；时间型判定交给巡检，验证通过");
    }
}
