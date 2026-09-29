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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 并发写同一条流水线：所有运行都必须被计数，不能有更新被覆盖或丢失。 */
class ConcurrentIngestTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentIngestTest.class);

    @Test
    void concurrentWritesToSamePipelineDoNotLoseUpdates(@TempDir Path dir) throws Exception {
        var props = TestSupport.props(dir, Duration.ofHours(10), Duration.ofHours(10),
                1000, Duration.ofHours(1));
        FileStateStore store = new FileStateStore(props);
        DefaultPipelineStateEngine engine = new DefaultPipelineStateEngine();
        engine.loadFrom(store.loadPipelineStates());
        DefaultAlertService alerts = new DefaultAlertService(store);
        alerts.load();
        var svc = new DefaultObservabilityService(new JacksonEventParser(), engine, alerts,
                store, new FileAuditLog(store), props);

        int threads = 16;
        int perThread = 25;
        Instant base = Instant.parse("2026-09-29T10:00:00Z");
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            int ti = t;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        int idx = ti * perThread + i;
                        String run = "run-" + idx;
                        Instant ts = base.plusSeconds(idx * 10L);
                        RunEvent s = TestSupport.ev("hot-pipe", run, EventType.STARTED, ts, null);
                        RunEvent f = TestSupport.ev("hot-pipe", run, EventType.FAILED,
                                ts.plusSeconds(5), "err-" + idx);
                        svc.ingest(s, ts.plusSeconds(6));
                        svc.ingest(f, ts.plusSeconds(7));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        log.info(">>> {} 个线程并发写同一条流水线，每个线程 {} 个失败运行", threads, perThread);
        start.countDown();
        for (var f : futures) {
            f.get(2, TimeUnit.MINUTES);
        }
        pool.shutdown();

        PipelineStatusSnapshot snap = svc.status("hot-pipe", base.plusSeconds(100000));
        TestSupport.logSnapshot(log, snap);
        log.info("判定记录条数={}", svc.decisions("hot-pipe").size());

        int totalRuns = threads * perThread;
        assertEquals(totalRuns, snap.totalFailures(), "全部失败运行必须被计数，不能丢更新");
        assertEquals(totalRuns, snap.consecutiveFailures());
        assertTrue(svc.decisions("hot-pipe").size() >= totalRuns * 2);
    }
}
