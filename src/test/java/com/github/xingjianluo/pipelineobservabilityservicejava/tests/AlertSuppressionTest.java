package com.github.xingjianluo.pipelineobservabilityservicejava.tests;

import com.github.xingjianluo.pipelineobservabilityservicejava.model.PipelineState;
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
 * 告警规则专项：
 * 1) 连续失败 / 延迟完成 / 长时间无成功 三类严格区分，条数分别可断言；
 * 2) 同一问题抑制窗口内只提醒一次，越过窗口且问题持续再提醒；
 * 3) 恢复后重新触发；延迟完成按 run 区分。
 */
class AlertSuppressionTest {

    private static final Logger log = LoggerFactory.getLogger(AlertSuppressionTest.class);
    private static final long T = 1_700_000_000_000L;

    /** 阈值：SLA=10min，新鲜度=60min，连续失败=3，抑制窗口=30min。 */
    private TestServiceFactory.Harness harness(Path dir, long now) {
        return TestServiceFactory.create(dir.toString(), new MutableClock(now), new CollectingAlertSink());
    }

    private void run(TestServiceFactory.Harness h, String pipeline, String run, String type, long start, long end) {
        log.info("[输入事件] {}/{} STARTED@{} -> {}@{}", pipeline, run, start, type, end);
        h.service().ingestRaw(Events.json(run + "-s", pipeline, run, "STARTED", start));
        h.service().ingestRaw(Events.json(run + "-e", pipeline, run, type, end));
    }

    @Test
    void delayedCompletionFiresOncePerRunEvenIfSlowSuccess(@TempDir Path dir) {
        var h = harness(dir, T + 20 * 60_000);
        // 一次"成功但很慢"的 run（15min > SLA 10min）：只能产生 DELAYED_COMPLETION，不能误报失败
        run(h, "p", "r1", "SUCCESS", T, T + 15 * 60_000);

        log.info("[判定依据] DELAYED={} CONSEC_FAIL={} STALE={}",
                h.alerts().countOf("DELAYED_COMPLETION"),
                h.alerts().countOf("CONSECUTIVE_FAILURES"),
                h.alerts().countOf("STALE_NO_SUCCESS"));
        assertEquals(1, h.alerts().countOf("DELAYED_COMPLETION"), "慢成功应触发延迟完成告警");
        assertEquals(0, h.alerts().countOf("CONSECUTIVE_FAILURES"), "成功不算连续失败");

        // 对同一 run 重复投递终态：幂等，不再次告警
        h.service().ingestRaw(Events.json("r1-e", "p", "r1", "SUCCESS", T + 15 * 60_000));
        assertEquals(1, h.alerts().countOf("DELAYED_COMPLETION"));

        // 另一个慢 run：作用域是 runId，应作为新问题各提醒一次
        run(h, "p", "r2", "FAILED", T + 20 * 60_000, T + 40 * 60_000);
        log.info("[判定依据] 两个 run 后 DELAYED={}（期望2）CONSEC_FAIL={}（期望0，未达3次）",
                h.alerts().countOf("DELAYED_COMPLETION"),
                h.alerts().countOf("CONSECUTIVE_FAILURES"));
        assertEquals(2, h.alerts().countOf("DELAYED_COMPLETION"));
        assertEquals(0, h.alerts().countOf("CONSECUTIVE_FAILURES"));
    }

    @Test
    void consecutiveFailuresDistinctFromDelayed(@TempDir Path dir) {
        var h = harness(dir, T + 200 * 60_000);
        // 三次"快速失败"（每次仅 1min，不超 SLA）：只应触发 CONSECUTIVE_FAILURES
        for (int i = 1; i <= 3; i++) {
            long base = T + i * 60_000L;
            run(h, "p", "r" + i, "FAILED", base, base + 60_000);
        }
        log.info("[判定依据] CONSEC_FAIL={}（期望1）DELAYED={}（期望0）",
                h.alerts().countOf("CONSECUTIVE_FAILURES"),
                h.alerts().countOf("DELAYED_COMPLETION"));
        assertEquals(1, h.alerts().countOf("CONSECUTIVE_FAILURES"));
        assertEquals(0, h.alerts().countOf("DELAYED_COMPLETION"));
        assertNotNull(h.alerts().all().get(0).message());
    }

    @Test
    void staleNoSuccessTriggeredByTimeScan(@TempDir Path dir) {
        var clock = new MutableClock(T);
        var alerts = new CollectingAlertSink();
        var h = TestServiceFactory.create(dir.toString(), clock, alerts);

        // 仅有一次失败、从未成功；事件驱动时还没超新鲜度（now=T+1min），不应报 stale
        clock.set(T + 60_000);
        run(h, "p", "r1", "FAILED", T, T + 30_000);
        assertEquals(0, alerts.countOf("STALE_NO_SUCCESS"));

        // 时间流逝超过 60min 新鲜度阈值，且仍无成功：巡检必须发现
        clock.set(T + 61 * 60_000);
        PipelineState state = h.store().get("p");
        // 直接调评估器模拟定时巡检
        h.evaluator().evaluateByTime(state);
        log.info("[判定依据] 越过新鲜度后 STALE={}（期望1）freshnessExceeded={}",
                alerts.countOf("STALE_NO_SUCCESS"),
                h.service().statusOf("p").freshnessExceeded());
        assertEquals(1, alerts.countOf("STALE_NO_SUCCESS"));
        assertTrue(h.service().statusOf("p").freshnessExceeded());

        // 巡检再来一次：抑制窗口内不重复
        h.evaluator().evaluateByTime(state);
        assertEquals(1, alerts.countOf("STALE_NO_SUCCESS"));

        // 一次成功（事件时间在当前时间附近）-> 恢复
        h.service().ingestRaw(Events.json("ok-s", "p", "r2", "STARTED", T + 61 * 60_000));
        h.service().ingestRaw(Events.json("ok-e", "p", "r2", "SUCCESS", T + 61 * 60_000 + 5000));
        log.info("[判定依据] 成功后 freshnessExceeded={}（期望false）",
                h.service().statusOf("p").freshnessExceeded());
        assertFalse(h.service().statusOf("p").freshnessExceeded());
    }

    @Test
    void suppressInWindowThenHeartbeatThenRecoverAndRefire(@TempDir Path dir) {
        var clock = new MutableClock(T);
        var alerts = new CollectingAlertSink();
        var h = TestServiceFactory.create(dir.toString(), clock, alerts);

        // 三连失败触发（间隔短，都在抑制窗口内）
        for (int i = 1; i <= 3; i++) {
            long base = T + i * 60_000L;
            clock.set(base + 60_000);
            run(h, "p", "r" + i, "FAILED", base, base + 60_000);
        }
        assertEquals(1, alerts.countOf("CONSECUTIVE_FAILURES"));

        // 窗口内继续失败：抑制
        long b4 = T + 10 * 60_000;
        clock.set(b4 + 60_000);
        run(h, "p", "r4", "FAILED", b4, b4 + 60_000);
        assertEquals(1, alerts.countOf("CONSECUTIVE_FAILURES"));

        // 越过 30min 抑制窗口仍在失败：心跳再提醒一次
        long b5 = T + 45 * 60_000;
        clock.set(b5 + 60_000);
        run(h, "p", "r5", "FAILED", b5, b5 + 60_000);
        log.info("[判定依据] 越过窗口 CONSEC_FAIL={}（期望2）", alerts.countOf("CONSECUTIVE_FAILURES"));
        assertEquals(2, alerts.countOf("CONSECUTIVE_FAILURES"));

        // 成功恢复
        long b6 = T + 46 * 60_000;
        clock.set(b6 + 60_000);
        run(h, "p", "r6", "SUCCESS", b6, b6 + 60_000);
        assertEquals(0, h.service().statusOf("p").consecutiveFailures());

        // 恢复后立即再来 3 连失败：必须重新触发（第 3 条告警）
        for (int i = 7; i <= 9; i++) {
            long base = T + (50 + i) * 60_000L;
            clock.set(base + 60_000);
            run(h, "p", "r" + i, "FAILED", base, base + 60_000);
        }
        log.info("[判定依据] 恢复后重触发 CONSEC_FAIL={}（期望3）", alerts.countOf("CONSECUTIVE_FAILURES"));
        assertEquals(3, alerts.countOf("CONSECUTIVE_FAILURES"));
    }
}
