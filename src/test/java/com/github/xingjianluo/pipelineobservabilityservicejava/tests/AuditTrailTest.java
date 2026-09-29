package com.github.xingjianluo.pipelineobservabilityservicejava.tests;

import com.github.xingjianluo.pipelineobservabilityservicejava.alert.DefaultAlertService;
import com.github.xingjianluo.pipelineobservabilityservicejava.audit.FileAuditLog;
import com.github.xingjianluo.pipelineobservabilityservicejava.engine.DefaultPipelineStateEngine;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.EventType;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RejectedEventException;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 事后可查：每个事件的输入、判定依据、告警都要在审计记录里。被拒绝的也要留痕。 */
class AuditTrailTest {

    private static final Logger log = LoggerFactory.getLogger(AuditTrailTest.class);

    @Test
    void everyDecisionIsRecordedIncludingRejections(@TempDir Path dir) {
        var props = TestSupport.props(dir, Duration.ofMinutes(30), Duration.ofMinutes(10),
                2, Duration.ofMinutes(15));
        FileStateStore store = new FileStateStore(props);
        DefaultPipelineStateEngine engine = new DefaultPipelineStateEngine();
        engine.loadFrom(store.loadPipelineStates());
        DefaultAlertService alerts = new DefaultAlertService(store);
        alerts.load();
        FileAuditLog audit = new FileAuditLog(store);
        var svc = new DefaultObservabilityService(new JacksonEventParser(), engine, alerts,
                store, audit, props);

        Instant t0 = Instant.parse("2026-09-29T10:00:00Z");
        String p = "pipe-audit";
        svc.ingest(TestSupport.ev(p, "r1", EventType.STARTED, t0, null), t0.plusSeconds(1));
        svc.ingest(TestSupport.ev(p, "r1", EventType.FAILED, t0.plusSeconds(5), "boom"),
                t0.plusSeconds(6));
        // 重复事件也有记录，outcome 标明 DUPLICATE。
        svc.ingest(TestSupport.ev(p, "r1", EventType.FAILED, t0.plusSeconds(5), "boom"),
                t0.plusSeconds(7));

        // 被拒绝事件不能丢：在审计里以 REJECTED:* 留痕，并继续抛异常给调用方。
        try {
            svc.ingest("{\"pipelineName\":\"" + p + "\"}", t0.plusSeconds(8));
        } catch (RejectedEventException expected) {
            log.info("如预期被拒绝: {}", expected.getReason());
        }

        var records = audit.findByPipeline(p);
        var rejected = audit.findAll().stream()
                .filter(r -> r.outcome().startsWith("REJECTED")).toList();
        log.info("流水线 {} 判定记录 {} 条：", p, records.size());
        records.forEach(r -> log.info("    {} eventTime={} outcome={} reasons={} alerts={}",
                r.eventType(), r.eventTime(), r.outcome(), r.reasons(), r.firedAlerts()));
        log.info("被拒绝事件留痕 {} 条", rejected.size());

        assertEquals(3, records.size());
        assertTrue(records.stream().anyMatch(r -> "DUPLICATE".equals(r.outcome())));
        assertTrue(records.stream().anyMatch(r -> !r.reasons().isEmpty()
                && r.reasons().get(0).contains("新建运行实例")));
        assertEquals(1, rejected.size());
        assertTrue(rejected.get(0).outcome().contains("MISSING_RUN_ID"));

        // 重启后审计记录仍在（追加式持久化）。
        FileStateStore store2 = new FileStateStore(props);
        FileAuditLog audit2 = new FileAuditLog(store2);
        assertEquals(4, audit2.findAll().size());
    }
}
