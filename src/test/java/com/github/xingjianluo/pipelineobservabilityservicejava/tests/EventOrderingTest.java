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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重点：重复、乱序、迟到事件。
 * 核心断言：同一批事件无论到达顺序/是否重复，最终派生状态完全一致。
 */
class EventOrderingTest {

    private static final Logger log = LoggerFactory.getLogger(EventOrderingTest.class);
    private static final long T = 1_700_000_000_000L;

    private TestServiceFactory.Harness harness(Path dir, MutableClock clock) {
        return TestServiceFactory.create(dir.toString(), clock, new CollectingAlertSink());
    }

    private void feed(TestServiceFactory.Harness h, List<String> events) {
        for (String json : events) {
            log.info("[输入事件] {}", json);
            h.service().ingestRaw(json);
        }
    }

    @Test
    void duplicateEventsAreIdempotent(@TempDir Path dir) {
        var clock = new MutableClock(T + 120_000);
        var h = harness(dir, clock);
        String e1 = Events.json("ev-1", "p", "run-1", "STARTED", T);
        String e2 = Events.json("ev-2", "p", "run-1", "SUCCESS", T + 60_000);

        feed(h, List.of(e1, e2, e1, e2, e2)); // 后三条全部重复
        PipelineDerivedStatus s = h.service().statusOf("p");
        log.info("[判定依据] lastSuccessAt={} consecutiveFailures={} freshnessExceeded={} acceptedEventCount={}",
                s.lastSuccessAt(), s.consecutiveFailures(), s.freshnessExceeded(),
                h.store().get("p").getAcceptedEventIds().size());

        assertEquals(T + 60_000, s.lastSuccessAt());
        assertEquals(0, s.consecutiveFailures());
        assertFalse(s.freshnessExceeded());
        assertEquals(2, h.store().get("p").getAcceptedEventIds().size(), "重复 eventId 只接受一次");
    }

    @Test
    void shuffledOrderYieldsSameFinalState(@TempDir Path dir) {
        var clock = new MutableClock(T + 120_000);
        var h = harness(dir, clock);

        // 两条流水线的事件集合完全相同，仅到达顺序不同（含乱序与重复）
        List<String> normal = List.of(
                Events.json("a1", "normal", "r1", "STARTED", T),
                Events.json("a2", "normal", "r1", "RETRIED", T + 30_000),
                Events.json("a3", "normal", "r1", "FAILED", T + 120_000),
                Events.json("a4", "normal", "r2", "STARTED", T + 200_000),
                Events.json("a5", "normal", "r2", "SUCCESS", T + 260_000));
        List<String> shuffled = List.of(
                Events.json("a5", "shuffled", "r2", "SUCCESS", T + 260_000), // 终态先到
                Events.json("a3", "shuffled", "r1", "FAILED", T + 120_000),
                Events.json("a2", "shuffled", "r1", "RETRIED", T + 30_000),
                Events.json("a4", "shuffled", "r2", "STARTED", T + 200_000),  // STARTED 迟到
                Events.json("a1", "shuffled", "r1", "STARTED", T),
                Events.json("a3", "shuffled", "r1", "FAILED", T + 120_000));   // 重复

        feed(h, normal);
        feed(h, shuffled);

        PipelineDerivedStatus sn = h.service().statusOf("normal");
        PipelineDerivedStatus ss = h.service().statusOf("shuffled");
        log.info("[判定依据-normal] lastSuccessAt={} lastRunFinishedAt={} consecutiveFailures={}",
                sn.lastSuccessAt(), sn.lastRunFinishedAt(), sn.consecutiveFailures());
        log.info("[判定依据-shuffle] lastSuccessAt={} lastRunFinishedAt={} consecutiveFailures={}",
                ss.lastSuccessAt(), ss.lastRunFinishedAt(), ss.consecutiveFailures());

        assertEquals(sn.lastSuccessAt(), ss.lastSuccessAt());
        assertEquals(sn.lastRunFinishedAt(), ss.lastRunFinishedAt());
        assertEquals(sn.consecutiveFailures(), ss.consecutiveFailures());
        assertEquals(0, sn.consecutiveFailures(), "最近一次 run 成功，连续失败计数应为 0");
        assertEquals(1, h.store().get("shuffled").getRuns().get("r1").getAttemptCount());
    }

    @Test
    void lateEarlierTerminalDoesNotOverrideLaterTerminal(@TempDir Path dir) {
        var clock = new MutableClock(T + 200_000);
        var h = harness(dir, clock);

        // FAILED 先到（更晚），更早的 SUCCESS 迟到，不能把结论改回成功
        feed(h, List.of(
                Events.json("b1", "p", "r", "STARTED", T),
                Events.json("b2", "p", "r", "FAILED", T + 100_000),
                Events.json("b3", "p", "r", "SUCCESS", T + 50_000)));

        PipelineDerivedStatus s = h.service().statusOf("p");
        log.info("[判定依据] lastSuccessAt={} consecutiveFailures={}（迟到的更早 SUCCESS 不应覆盖更晚 FAILED）",
                s.lastSuccessAt(), s.consecutiveFailures());
        assertNull(s.lastSuccessAt());
        assertEquals(1, s.consecutiveFailures());
    }

    @Test
    void laterTerminalArrivingLateOverridesEarlierOutcome(@TempDir Path dir) {
        var clock = new MutableClock(T + 200_000);
        var h = harness(dir, clock);

        // SUCCESS 先到（早），更晚的 TIMED_OUT 迟到，应覆盖为超时
        feed(h, List.of(
                Events.json("c1", "p", "r", "STARTED", T),
                Events.json("c2", "p", "r", "SUCCESS", T + 50_000),
                Events.json("c3", "p", "r", "TIMED_OUT", T + 100_000)));

        PipelineDerivedStatus s = h.service().statusOf("p");
        log.info("[判定依据] lastSuccessAt={} consecutiveFailures={}（更晚的 TIMED_OUT 迟到应覆盖 SUCCESS）",
                s.lastSuccessAt(), s.consecutiveFailures());
        assertNull(s.lastSuccessAt());
        assertEquals(1, s.consecutiveFailures());
    }

    @Test
    void sameTimestampTerminalUsesPriority(@TempDir Path dir) {
        var clock = new MutableClock(T + 200_000);
        var h = harness(dir, clock);

        // 同一 eventTime 的 SUCCESS 与 TIMED_OUT：优先级 TIMED_OUT > FAILED > SUCCESS
        feed(h, List.of(
                Events.json("d1", "p", "r", "STARTED", T),
                Events.json("d2", "p", "r", "SUCCESS", T + 60_000),
                Events.json("d3", "p", "r", "TIMED_OUT", T + 60_000)));

        PipelineDerivedStatus s = h.service().statusOf("p");
        log.info("[判定依据] lastSuccessAt={} consecutiveFailures={}（同时间终态按 TIMED_OUT 优先）",
                s.lastSuccessAt(), s.consecutiveFailures());
        assertNull(s.lastSuccessAt());
        assertEquals(1, s.consecutiveFailures());
    }

    @Test
    void lateStartedEventCorrectsSlaDuration(@TempDir Path dir) {
        var clock = new MutableClock(T + 200_000);
        var h = harness(dir, clock);

        // 终态先到，STARTED 迟到；SLA 必须依据修正后的 startedAt 判定（SLA=10min，耗时 15min）
        feed(h, List.of(
                Events.json("e2", "p", "r", "SUCCESS", T + 15 * 60_000),
                Events.json("e1", "p", "r", "STARTED", T)));

        PipelineDerivedStatus s = h.service().statusOf("p");
        log.info("[判定依据] slaExceededOnLastRun={} durationMs={} slaMs={}（迟到 STARTED 修正开始时间）",
                s.slaExceededOnLastRun(), 15 * 60_000L, s.slaThresholdMs());
        assertTrue(s.slaExceededOnLastRun());
    }
}
