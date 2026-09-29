package com.github.xingjianluo.pipelineobservabilityservicejava.tests;

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

import static com.github.xingjianluo.pipelineobservabilityservicejava.tests.TestSupport.ev;
import static com.github.xingjianluo.pipelineobservabilityservicejava.tests.TestSupport.evWithId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** 重点：重复、乱序、迟到事件都不能改变最终判定。 */
class EventOrderingTest {

    private static final Logger log = LoggerFactory.getLogger(EventOrderingTest.class);

    private DefaultObservabilityService newService(Path dir) {
        var props = TestSupport.props(dir, Duration.ofMinutes(30), Duration.ofMinutes(10),
                3, Duration.ofMinutes(15));
        FileStateStore store = new FileStateStore(props);
        DefaultPipelineStateEngine engine = new DefaultPipelineStateEngine();
        engine.loadFrom(store.loadPipelineStates());
        DefaultAlertService alerts = new DefaultAlertService(store);
        alerts.load();
        return new DefaultObservabilityService(new JacksonEventParser(), engine, alerts,
                store, new FileAuditLog(store), props);
    }

    @Test
    void duplicateAndOutOfOrderEventsProduceSameFinalState(@TempDir Path dir) {
        var svc = newService(dir);
        Instant t0 = Instant.parse("2026-09-29T10:00:00Z");
        String p = "pipe-order";

        // 乱序发送：FAILED 先到，STARTED 后到，再补一条更早的 SUCCEEDED（纠正终态）。
        var failed = ev(p, "run-1", EventType.FAILED, t0.plusSeconds(120), "disk full");
        var started = ev(p, "run-1", EventType.STARTED, t0, null);
        var succeeded = ev(p, "run-1", EventType.SUCCEEDED, t0.plusSeconds(100), null);

        log.info("== 场景1：乱序 + 更早成功纠正 ==");
        for (RunEvent e : new RunEvent[]{failed, started, succeeded}) {
            TestSupport.logEvent(log, e);
            var r = svc.ingest(e, e.receivedAt());
            log.info("   outcome={} reasons={}", r.outcome(), r.reasons());
        }
        // 重复发送完全一样的事件若干次（含内容指纹去重和显式 eventId 去重）。
        log.info("== 场景2：重复事件 ==");
        for (int i = 0; i < 3; i++) {
            TestSupport.logEvent(log, succeeded);
            var r = svc.ingest(succeeded, succeeded.receivedAt());
            assertEquals("DUPLICATE", r.outcome());
            log.info("   outcome={} reasons={}", r.outcome(), r.reasons());
        }
        var withId1 = evWithId(p, "run-1", EventType.FAILED, t0.plusSeconds(200), "again", "e-200");
        var withId2 = evWithId(p, "run-1", EventType.FAILED, t0.plusSeconds(200), "again", "e-200");
        TestSupport.logEvent(log, withId1);
        svc.ingest(withId1, withId1.receivedAt());
        TestSupport.logEvent(log, withId2);
        assertEquals("DUPLICATE", svc.ingest(withId2, withId2.receivedAt()).outcome());

        PipelineStatusSnapshot s = svc.status(p, t0.plusSeconds(201));
        TestSupport.logSnapshot(log, s);
        TestSupport.printDecisions(log, svc, p);

        // 最早终态 SUCCEEDED 生效：运行算成功，连续失败为 0。
        assertEquals(t0.plusSeconds(100), s.lastSuccessAt());
        assertEquals(0, s.consecutiveFailures());
        assertEquals(1, s.totalSuccesses());
        assertFalse(s.failureThresholdExceeded());
    }

    @Test
    void lateEventAfterFinalStateIsAbsorbed(@TempDir Path dir) {
        var svc = newService(dir);
        Instant t0 = Instant.parse("2026-09-29T11:00:00Z");
        String p = "pipe-late";

        var started = ev(p, "run-late", EventType.STARTED, t0, null);
        var succeeded = ev(p, "run-late", EventType.SUCCEEDED, t0.plusSeconds(30), null);
        // 迟到的 STARTED（eventTime 早于终态）：不能把成功覆盖回运行中。
        var lateStarted = ev(p, "run-late", EventType.STARTED, t0.plusSeconds(5), null);
        // 迟到的 RETRYING 也不能改写。
        var lateRetry = ev(p, "run-late", EventType.RETRYING, t0.plusSeconds(10), null);

        for (RunEvent e : new RunEvent[]{started, succeeded, lateStarted, lateRetry}) {
            TestSupport.logEvent(log, e);
            var r = svc.ingest(e, e.receivedAt());
            log.info("   outcome={} reasons={}", r.outcome(), r.reasons());
        }

        PipelineStatusSnapshot s = svc.status(p, t0.plusSeconds(60));
        TestSupport.logSnapshot(log, s);
        TestSupport.printDecisions(log, svc, p);
        assertEquals(t0.plusSeconds(30), s.lastSuccessAt());
        assertEquals(0, s.consecutiveFailures());
    }

    @Test
    void consecutiveFailuresCountedByCompletionOrderRegardlessOfArrival(@TempDir Path dir) {
        var svc = newService(dir);
        Instant t0 = Instant.parse("2026-09-29T12:00:00Z");
        String p = "pipe-fails";

        // 三个运行按完成时间 f1 < f2 < f3，但事件到达顺序打乱。
        var f1 = ev(p, "r1", EventType.FAILED, t0.plusSeconds(60), "e1");
        var f3 = ev(p, "r3", EventType.FAILED, t0.plusSeconds(180), "e3");
        var f2 = ev(p, "r2", EventType.FAILED, t0.plusSeconds(120), "e2");
        var s3 = ev(p, "r3", EventType.STARTED, t0.plusSeconds(170), null);
        var s2 = ev(p, "r2", EventType.STARTED, t0.plusSeconds(110), null);
        var s1 = ev(p, "r1", EventType.STARTED, t0.plusSeconds(50), null);

        for (RunEvent e : new RunEvent[]{f1, f3, f2, s3, s2, s1}) {
            TestSupport.logEvent(log, e);
            var r = svc.ingest(e, e.receivedAt());
            log.info("   outcome={} reasons={}", r.outcome(), r.reasons());
        }

        PipelineStatusSnapshot s = svc.status(p, t0.plusSeconds(200));
        TestSupport.logSnapshot(log, s);
        TestSupport.printDecisions(log, svc, p);
        assertEquals(3, s.consecutiveFailures());
        assertEquals(3, s.totalFailures());
    }
}
