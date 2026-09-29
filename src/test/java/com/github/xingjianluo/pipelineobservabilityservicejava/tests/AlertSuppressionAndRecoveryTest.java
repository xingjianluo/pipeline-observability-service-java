package com.github.xingjianluo.pipelineobservabilityservicejava.tests;

import com.github.xingjianluo.pipelineobservabilityservicejava.alert.Alert;
import com.github.xingjianluo.pipelineobservabilityservicejava.alert.AlertType;
import com.github.xingjianluo.pipelineobservabilityservicejava.alert.DefaultAlertService;
import com.github.xingjianluo.pipelineobservabilityservicejava.audit.FileAuditLog;
import com.github.xingjianluo.pipelineobservabilityservicejava.engine.DefaultPipelineStateEngine;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.EventType;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RunEvent;
import com.github.xingjianluo.pipelineobservabilityservicejava.ingest.JacksonEventParser;
import com.github.xingjianluo.pipelineobservabilityservicejava.service.DefaultObservabilityService;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineStatusSnapshot;
import com.github.xingjianluo.pipelineobservabilityservicejava.store.FileStateStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 告警：抑制窗口内一次、恢复后可重新触发、三类问题互不混淆。 */
class AlertSuppressionAndRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(AlertSuppressionAndRecoveryTest.class);

    private DefaultObservabilityService newService(Path dir) {
        // 新鲜度 30s，SLA 10s，连续失败阈值 2，抑制窗口 15s。
        var props = TestSupport.props(dir, Duration.ofSeconds(30), Duration.ofSeconds(10),
                2, Duration.ofSeconds(15));
        FileStateStore store = new FileStateStore(props);
        DefaultPipelineStateEngine engine = new DefaultPipelineStateEngine();
        engine.loadFrom(store.loadPipelineStates());
        DefaultAlertService alerts = new DefaultAlertService(store);
        alerts.load();
        return new DefaultObservabilityService(new JacksonEventParser(), engine, alerts,
                store, new FileAuditLog(store), props);
    }

    private long count(List<Alert> alerts, AlertType t) {
        return alerts.stream().filter(a -> a.type() == t).count();
    }

    @Test
    void suppressWithinWindowAndRefireAfterRecovery(@TempDir Path dir) {
        var svc = newService(dir);
        Instant t0 = Instant.parse("2026-09-29T10:00:00Z");
        String p = "pipe-alert";

        // 两次失败 -> 连续失败告警首次触发。
        log.info("== 首次触发连续失败告警 ==");
        feed(svc, p, "r1", t0, EventType.FAILED, "e1");
        feed(svc, p, "r2", t0.plusSeconds(5), EventType.FAILED, "e2");
        assertEquals(1, count(svc.alerts(p), AlertType.CONSECUTIVE_FAILURES));

        // 抑制窗口内持续失败：不再发。
        log.info("== 抑制窗口内再来失败，不重复提醒 ==");
        feed(svc, p, "r3", t0.plusSeconds(10), EventType.FAILED, "e3");
        feed(svc, p, "r4", t0.plusSeconds(12), EventType.FAILED, "e4");
        assertEquals(1, count(svc.alerts(p), AlertType.CONSECUTIVE_FAILURES));

        // 恢复：成功一次，连续失败清零。
        log.info("== 成功恢复 ==");
        RunEvent ok = TestSupport.ev(p, "r5", EventType.SUCCEEDED, t0.plusSeconds(20), null);
        TestSupport.logEvent(log, ok);
        svc.ingest(ok, t0.plusSeconds(21));
        PipelineStatusSnapshot recovered = svc.status(p, t0.plusSeconds(21));
        TestSupport.logSnapshot(log, recovered);
        assertEquals(0, recovered.consecutiveFailures());
        assertFalse(recovered.activeAlertTypes().contains(AlertType.CONSECUTIVE_FAILURES.name()));

        // 抑制窗口未过也没关系：已经恢复（active=false），再次连续失败立即重新触发。
        log.info("== 恢复后再次连续失败，应重新触发 ==");
        feed(svc, p, "r6", t0.plusSeconds(25), EventType.FAILED, "e6");
        feed(svc, p, "r7", t0.plusSeconds(28), EventType.FAILED, "e7");
        List<Alert> all = svc.alerts(p);
        long failAlerts = count(all, AlertType.CONSECUTIVE_FAILURES);
        log.info("连续失败告警总条数={}", failAlerts);
        assertEquals(2, failAlerts, "恢复后必须能重新触发");
        TestSupport.printDecisions(log, svc, p);
    }

    @Test
    void threeKindsOfProblemsAreDistinguished(@TempDir Path dir) {
        var svc = newService(dir);
        Instant t0 = Instant.parse("2026-09-29T10:00:00Z");

        // 1) 延迟完成：运行 20s > SLA 10s，但成功了 -> SLA_BREACHED，不是失败告警。
        String sla = "pipe-sla";
        RunEvent st = TestSupport.ev(sla, "s1", EventType.STARTED, t0, null);
        RunEvent okLate = TestSupport.ev(sla, "s1", EventType.SUCCEEDED, t0.plusSeconds(20), null);
        TestSupport.logEvent(log, st);
        svc.ingest(st, t0.plusSeconds(1));
        TestSupport.logEvent(log, okLate);
        svc.ingest(okLate, t0.plusSeconds(21));
        PipelineStatusSnapshot slaSnap = svc.status(sla, t0.plusSeconds(21));
        TestSupport.logSnapshot(log, slaSnap);
        assertTrue(slaSnap.slaBreached());
        assertEquals(0, slaSnap.consecutiveFailures());
        assertEquals(1, count(svc.alerts(sla), AlertType.SLA_BREACHED));
        assertEquals(0, count(svc.alerts(sla), AlertType.CONSECUTIVE_FAILURES));

        // 2) 长时间没有成功：最后成功后超过新鲜度阈值 -> STALE_NO_SUCCESS。
        String stale = "pipe-stale";
        RunEvent good = TestSupport.ev(stale, "g1", EventType.SUCCEEDED, t0, null);
        TestSupport.logEvent(log, good);
        svc.ingest(good, t0.plusSeconds(1));
        // 时间流逝（无新事件），后台巡检等价调用：
        log.info(">>> 时间推进到 lastSuccess+40s，触发新鲜度巡检");
        svc.evaluateTimeBasedAlerts(t0.plusSeconds(40));
        PipelineStatusSnapshot staleSnap = svc.status(stale, t0.plusSeconds(40));
        TestSupport.logSnapshot(log, staleSnap);
        assertTrue(staleSnap.stale());
        assertEquals(1, count(svc.alerts(stale), AlertType.STALE_NO_SUCCESS));
        assertEquals(0, count(svc.alerts(stale), AlertType.SLA_BREACHED));
        assertEquals(0, count(svc.alerts(stale), AlertType.CONSECUTIVE_FAILURES));

        // 3) 恢复新鲜度（新的成功）后告警复位；再变 stale 可重触发。
        RunEvent good2 = TestSupport.ev(stale, "g2", EventType.SUCCEEDED, t0.plusSeconds(50), null);
        TestSupport.logEvent(log, good2);
        svc.ingest(good2, t0.plusSeconds(51));
        assertFalse(svc.status(stale, t0.plusSeconds(51)).stale());
        svc.evaluateTimeBasedAlerts(t0.plusSeconds(90));
        assertEquals(2, count(svc.alerts(stale), AlertType.STALE_NO_SUCCESS),
                "恢复后再次 stale 应重新触发");
        log.info("STALE 告警历史条数=2 验证通过");
    }

    @Test
    void suppressedOnlyOnceEvenWhenEvaluatedRepeatedly(@TempDir Path dir) {
        var svc = newService(dir);
        Instant t0 = Instant.parse("2026-09-29T10:00:00Z");
        String p = "pipe-suppress";
        feed(svc, p, "r1", t0, EventType.FAILED, "e1");
        feed(svc, p, "r2", t0.plusSeconds(1), EventType.FAILED, "e2");
        // 抑制窗口内反复巡检，不能多报。
        for (int i = 2; i <= 14; i++) {
            svc.evaluateTimeBasedAlerts(t0.plusSeconds(i));
        }
        assertEquals(1, svc.alerts(p).size());
        // 超过抑制窗口且问题仍在：允许再提醒一次。
        svc.evaluateTimeBasedAlerts(t0.plusSeconds(20));
        assertEquals(2, svc.alerts(p).size());
        log.info("抑制窗口内只报一次、窗口过后再提醒一次，共 {} 条", svc.alerts(p).size());
    }

    @Test
    void runningPastSlaIsDetectedButOnlyWithRecentActivity(@TempDir Path dir) {
        var svc = newService(dir);
        Instant t0 = Instant.parse("2026-09-29T10:00:00Z");
        String p = "pipe-running-sla";

        // STARTED 后 12s 仍未结束（> SLA 10s），且最近活动时间(STARTED)也过了截止+无终态：巡检报 SLA。
        RunEvent st = TestSupport.ev(p, "z1", EventType.STARTED, t0, null);
        TestSupport.logEvent(log, st);
        svc.ingest(st, t0.plusSeconds(1));
        assertFalse(svc.status(p, t0.plusSeconds(5)).slaBreached(), "5s 时还没超 SLA");
        svc.evaluateTimeBasedAlerts(t0.plusSeconds(12));
        PipelineStatusSnapshot snap = svc.status(p, t0.plusSeconds(12));
        TestSupport.logSnapshot(log, snap);
        assertTrue(snap.runningPastSla());
        assertTrue(snap.slaBreached());
        assertEquals(1, count(svc.alerts(p), AlertType.SLA_BREACHED));
    }

    private void feed(DefaultObservabilityService svc, String p, String run,
                      Instant t, EventType terminal, String detail) {
        RunEvent s = TestSupport.ev(p, run, EventType.STARTED, t, null);
        RunEvent f = TestSupport.ev(p, run, terminal, t.plusSeconds(2), detail);
        TestSupport.logEvent(log, s);
        svc.ingest(s, t.plusSeconds(3));
        TestSupport.logEvent(log, f);
        svc.ingest(f, t.plusSeconds(4));
    }
}
