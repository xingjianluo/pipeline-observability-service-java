package com.github.xingjianluo.pipelineobservabilityservicejava.tests;

import com.github.xingjianluo.pipelineobservabilityservicejava.model.PipelineDerivedStatus;
import com.github.xingjianluo.pipelineobservabilityservicejava.support.CollectingAlertSink;
import com.github.xingjianluo.pipelineobservabilityservicejava.support.Events;
import com.github.xingjianluo.pipelineobservabilityservicejava.support.MutableClock;
import com.github.xingjianluo.pipelineobservabilityservicejava.support.TestServiceFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重启恢复：全部结论必须来自落盘状态，重启前后对得上，不能"一重启就当从没跑过"。
 * 覆盖：派生状态恢复、告警抑制跨重启延续、被拒事件恢复。
 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);
    private static final long T = 1_700_000_000_000L;

    @Test
    void statusSurvivesRestart(@TempDir Path dir) {
        var clock1 = new MutableClock(T + 30 * 60_000);
        var alerts1 = new CollectingAlertSink();
        var h1 = TestServiceFactory.create(dir.toString(), clock1, alerts1);

        // 两个 run：第一次失败，第二次成功（乱序喂入）
        h1.service().ingestRaw(Events.json("e2", "p", "r1", "FAILED", T + 10 * 60_000));
        h1.service().ingestRaw(Events.json("e1", "p", "r1", "STARTED", T));
        h1.service().ingestRaw(Events.json("e4", "p", "r2", "SUCCESS", T + 25 * 60_000));
        h1.service().ingestRaw(Events.json("e3", "p", "r2", "STARTED", T + 20 * 60_000));
        h1.service().ingestRaw("{broken-json"); // 一条被拒事件

        PipelineDerivedStatus before = h1.service().statusOf("p");
        log.info("[重启前] lastSuccessAt={} lastRunFinishedAt={} consecutiveFailures={} runs={} rejected={}",
                before.lastSuccessAt(), before.lastRunFinishedAt(), before.consecutiveFailures(),
                h1.store().get("p").getRuns().size(), h1.service().rejectedEvents().size());

        // ===== 模拟服务重启：同目录、新组件、内存为空，全部从磁盘恢复 =====
        var clock2 = new MutableClock(T + 30 * 60_000);
        var alerts2 = new CollectingAlertSink();
        var h2 = TestServiceFactory.restart(h1, clock2, alerts2);

        PipelineDerivedStatus after = h2.service().statusOf("p");
        log.info("[重启后] lastSuccessAt={} lastRunFinishedAt={} consecutiveFailures={} runs={} rejected={}",
                after.lastSuccessAt(), after.lastRunFinishedAt(), after.consecutiveFailures(),
                h2.store().get("p").getRuns().size(), h2.service().rejectedEvents().size());

        assertEquals(before.lastSuccessAt(), after.lastSuccessAt());
        assertEquals(before.lastRunFinishedAt(), after.lastRunFinishedAt());
        assertEquals(before.consecutiveFailures(), after.consecutiveFailures());
        assertEquals(2, h2.store().get("p").getRuns().size());
        assertEquals(4, h2.store().get("p").getAcceptedEventIds().size());
        assertEquals(1, h2.service().rejectedEvents().size(), "被拒事件也必须随重启恢复");

        // 重启后重复投递重启前已接受的事件，仍应幂等
        var dup = h2.service().ingestRaw(Events.json("e4", "p", "r2", "SUCCESS", T + 25 * 60_000));
        assertTrue(dup.isDuplicate());
        assertEquals(4, h2.store().get("p").getAcceptedEventIds().size());
    }

    @Test
    void alertSuppressionAndRecoverySurviveRestart(@TempDir Path dir) {
        // 抑制窗口 30min，连续失败阈值 3
        var clock1 = new MutableClock(T + 40 * 60_000);
        var alerts1 = new CollectingAlertSink();
        var h1 = TestServiceFactory.create(dir.toString(), clock1, alerts1);

        for (int i = 1; i <= 3; i++) {
            long base = T + i * 60_000L;
            log.info("[输入事件] run{} FAILED @ {}", i, base + 30_000);
            h1.service().ingestRaw(Events.json("s" + i, "p", "r" + i, "STARTED", base));
            h1.service().ingestRaw(Events.json("f" + i, "p", "r" + i, "FAILED", base + 30_000));
        }
        // 第 4 次失败：抑制窗口内，不应再提醒
        long b4 = T + 5 * 60_000L;
        h1.service().ingestRaw(Events.json("s4", "p", "r4", "STARTED", b4));
        h1.service().ingestRaw(Events.json("f4", "p", "r4", "FAILED", b4 + 30_000));

        long cfBeforeRestart = alerts1.countOf("CONSECUTIVE_FAILURES");
        log.info("[重启前] CONSECUTIVE_FAILURES 告警条数={}（期望 1，第4次被抑制）", cfBeforeRestart);
        assertEquals(1, cfBeforeRestart);

        // ===== 重启，当前时间仍在抑制窗口内 =====
        var clock2 = new MutableClock(T + 40 * 60_000);
        var alerts2 = new CollectingAlertSink();
        var h2 = TestServiceFactory.restart(h1, clock2, alerts2);
        long b5 = T + 6 * 60_000L;
        h2.service().ingestRaw(Events.json("s5", "p", "r5", "STARTED", b5));
        h2.service().ingestRaw(Events.json("f5", "p", "r5", "FAILED", b5 + 30_000));
        log.info("[重启后窗口内] CONSECUTIVE_FAILURES 告警条数={}（期望 0，抑制状态已随重启恢复）",
                alerts2.countOf("CONSECUTIVE_FAILURES"));
        assertEquals(0, alerts2.countOf("CONSECUTIVE_FAILURES"), "抑制窗口内重启后也不能重复提醒");

        // 推进到抑制窗口之外，问题仍持续：心跳式再提醒一次
        clock2.advance(31 * 60_000);
        long b6 = T + 40 * 60_000L;
        h2.service().ingestRaw(Events.json("s6", "p", "r6", "STARTED", b6));
        h2.service().ingestRaw(Events.json("f6", "p", "r6", "FAILED", b6 + 30_000));
        log.info("[越过抑制窗口] CONSECUTIVE_FAILURES 告警条数={}（期望 1）",
                alerts2.countOf("CONSECUTIVE_FAILURES"));
        assertEquals(1, alerts2.countOf("CONSECUTIVE_FAILURES"));

        // 成功 -> 恢复；再连续失败到阈值 -> 必须能重新触发
        long b7 = T + 42 * 60_000L;
        h2.service().ingestRaw(Events.json("s7", "p", "r7", "STARTED", b7));
        h2.service().ingestRaw(Events.json("ok7", "p", "r7", "SUCCESS", b7 + 10_000));
        log.info("[恢复] 成功后 CF 告警 active 应为 false");
        // 之后新的 3 连失败（时间都在同一抑制窗口之后，逐条评估）
        String[] firedRuns = {"r8", "r9", "r10"};
        for (int i = 0; i < 3; i++) {
            long base = T + (50 + i) * 60_000L;
            h2.service().ingestRaw(Events.json("s" + (8 + i), "p", firedRuns[i], "STARTED", base));
            h2.service().ingestRaw(Events.json("f" + (8 + i), "p", firedRuns[i], "FAILED", base + 30_000));
        }
        long total = alerts2.countOf("CONSECUTIVE_FAILURES");
        log.info("[恢复后重新触发] CONSECUTIVE_FAILURES 总条数={}（期望 2：心跳1 + 恢复后重触发1）", total);
        assertEquals(2, total, "问题恢复后再次达到阈值必须重新提醒");
    }
}
