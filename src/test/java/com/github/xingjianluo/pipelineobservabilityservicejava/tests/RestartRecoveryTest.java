package com.github.xingjianluo.pipelineobservabilityservicejava.tests;

import com.github.xingjianluo.pipelineobservabilityservicejava.alert.Alert;
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

/** 服务重启：用同一个存储目录重新构建全部组件，结论必须与重启前完全一致。 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);

    private DefaultObservabilityService boot(Path dir) {
        var props = TestSupport.props(dir, Duration.ofMinutes(30), Duration.ofMinutes(10),
                2, Duration.ofMinutes(15));
        FileStateStore store = new FileStateStore(props);
        DefaultPipelineStateEngine engine = new DefaultPipelineStateEngine();
        engine.loadFrom(store.loadPipelineStates());
        DefaultAlertService alerts = new DefaultAlertService(store);
        alerts.load();
        return new DefaultObservabilityService(new JacksonEventParser(), engine, alerts,
                store, new FileAuditLog(store), props);
    }

    @Test
    void stateSurvivesRestartAndRepeatedEventsAfterRestartAreDeduped(@TempDir Path dir) {
        Instant t0 = Instant.parse("2026-09-29T10:00:00Z");
        String p = "pipe-restart";

        // ---- 第一次"进程" ----
        var svc1 = boot(dir);
        RunEvent s1 = TestSupport.ev(p, "r1", EventType.STARTED, t0, null);
        RunEvent f1 = TestSupport.ev(p, "r1", EventType.FAILED, t0.plusSeconds(20), "e1");
        RunEvent s2 = TestSupport.ev(p, "r2", EventType.STARTED, t0.plusSeconds(60), null);
        RunEvent f2 = TestSupport.ev(p, "r2", EventType.FAILED, t0.plusSeconds(80), "e2");
        for (RunEvent e : new RunEvent[]{s1, f1, s2, f2}) {
            TestSupport.logEvent(log, e);
            svc1.ingest(e, e.receivedAt());
        }
        PipelineStatusSnapshot before = svc1.status(p, t0.plusSeconds(81));
        TestSupport.logSnapshot(log, before);
        List<Alert> alertsBefore = svc1.alerts(p);
        log.info("重启前告警条数={} {}", alertsBefore.size(),
                alertsBefore.stream().map(a -> a.type().name()).toList());

        // ---- 模拟重启：丢弃内存对象，用同一目录重新构建 ----
        log.info(">>>>>>>>>> 模拟服务重启，目录={}", dir);
        var svc2 = boot(dir);
        PipelineStatusSnapshot after = svc2.status(p, t0.plusSeconds(81));
        TestSupport.logSnapshot(log, after);

        assertEquals(before.lastSuccessAt(), after.lastSuccessAt());
        assertEquals(before.consecutiveFailures(), after.consecutiveFailures());
        assertEquals(before.totalFailures(), after.totalFailures());
        assertEquals(before.totalSuccesses(), after.totalSuccesses());
        assertEquals(2, after.consecutiveFailures());

        // 重启后重放同样的事件，仍然全部判为重复，不会重复计数/重复告警。
        int duplicates = 0;
        for (RunEvent e : new RunEvent[]{s1, f1, s2, f2}) {
            TestSupport.logEvent(log, e);
            var r = svc2.ingest(e, e.receivedAt());
            log.info("   outcome={}", r.outcome());
            if ("DUPLICATE".equals(r.outcome())) {
                duplicates++;
            }
        }
        assertEquals(4, duplicates);
        PipelineStatusSnapshot afterReplay = svc2.status(p, t0.plusSeconds(81));
        assertEquals(2, afterReplay.consecutiveFailures());
        // 抑制状态也从盘上恢复：同样条件不再补发告警。
        assertEquals(1, svc2.alerts(p).size(), "重启后不能重复发同一问题的告警");
        log.info("重启后重放完成，告警总条数={}", svc2.alerts(p).size());
        TestSupport.printDecisions(log, svc2, p);
    }
}
