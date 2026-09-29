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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** 并发写同一条流水线：状态不能被覆盖或丢更新。 */
class ConcurrentWriteTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentWriteTest.class);
    private static final long T = 1_700_000_000_000L;

    @Test
    void concurrentDistinctRunsNoLostUpdate(@TempDir Path dir) throws Exception {
        int threads = 32;
        var clock = new MutableClock(T + 30_000);
        var h = TestServiceFactory.create(dir.toString(), clock, new CollectingAlertSink());

        // 池大小必须等于任务数：所有任务都要先 park 在 start 门闩上，
        // 若池太小，排队任务没机会执行 ready.countDown()，会造成测试自身饥饿（非产品代码死锁）
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();

        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            tasks.add(() -> {
                try {
                    ready.countDown();
                    start.await();
                    String runId = "run-" + idx;
                    // 每个线程写自己的 run：STARTED + SUCCESS，且两个事件被 start 门闩挤在一起并发提交
                    String started = Events.json("ev-s-" + idx, "hot", runId, "STARTED", T + idx * 1000L);
                    String success = Events.json("ev-ok-" + idx, "hot", runId, "SUCCESS", T + idx * 1000L + 5000);
                    log.info("[输入事件] {}", started);
                    log.info("[输入事件] {}", success);
                    h.service().ingestRaw(started);
                    if (h.service().ingestRaw(success).isAccepted()) {
                        accepted.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        for (Runnable task : tasks) {
            pool.submit(task);
        }
        assertTrue(ready.await(10, TimeUnit.SECONDS), "所有并发任务应就绪");
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        PipelineDerivedStatus status = h.service().statusOf("hot");
        int runCount = h.store().get("hot").getRuns().size();
        int eventCount = h.store().get("hot").getAcceptedEventIds().size();
        log.info("[判定依据] 线程数={} 接受成功run数={} 落盘run数={} 落盘事件数={} lastSuccessAt={}",
                threads, accepted.get(), runCount, eventCount, status.lastSuccessAt());

        assertEquals(threads, accepted.get(), "每个 run 的 SUCCESS 都必须被计入，不能丢更新");
        assertEquals(threads, runCount, "每个 run 都必须存在，不能被并发覆盖");
        assertEquals(threads * 2, eventCount, "每条事件恰好接受一次");
        assertEquals(T + (threads - 1) * 1000L + 5000, status.lastSuccessAt());
        assertEquals(0, status.consecutiveFailures());
    }

    @Test
    void concurrentDuplicateSameEventAcceptedOnce(@TempDir Path dir) throws Exception {
        int threads = 16;
        var clock = new MutableClock(T + 30_000);
        var h = TestServiceFactory.create(dir.toString(), clock, new CollectingAlertSink());

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();
        // 同一个 eventId 被所有线程并发投递
        String event = Events.json("same-id", "dup", "r1", "SUCCESS", T);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    log.info("[输入事件] {}", event);
                    var r = h.service().ingestRaw(event);
                    if (r.isDuplicate()) {
                        duplicate.incrementAndGet();
                    } else if (r.isAccepted()) {
                        accepted.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        int eventCount = h.store().get("dup").getAcceptedEventIds().size();
        log.info("[判定依据] 并发重复投递={} 首次接受={} 判重={} 落盘事件数={}",
                threads, accepted.get(), duplicate.get(), eventCount);
        assertEquals(1, accepted.get());
        assertEquals(threads - 1, duplicate.get());
        assertEquals(1, eventCount, "同一 eventId 并发下也只能落盘一次");
    }
}
