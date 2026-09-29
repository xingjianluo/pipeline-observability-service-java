package com.github.xingjianluo.pipelineobservabilityservicejava.tests;

import com.github.xingjianluo.pipelineobservabilityservicejava.event.EventIngestResult;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.RejectReason;
import com.github.xingjianluo.pipelineobservabilityservicejava.support.CollectingAlertSink;
import com.github.xingjianluo.pipelineobservabilityservicejava.support.Events;
import com.github.xingjianluo.pipelineobservabilityservicejava.support.MutableClock;
import com.github.xingjianluo.pipelineobservabilityservicejava.support.TestServiceFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** 失败/字段缺失的事件按不同原因拒绝，不静默丢弃，且全部可经 rejected 查询留痕。 */
class EventValidationTest {

    private static final Logger log = LoggerFactory.getLogger(EventValidationTest.class);
    private static final long T = 1_700_000_000_000L;

    private TestServiceFactory.Harness harness(Path dir) {
        return TestServiceFactory.create(dir.toString(), new MutableClock(T), new CollectingAlertSink());
    }

    private EventIngestResult reject(TestServiceFactory.Harness h, String raw, RejectReason expected) {
        log.info("[输入事件] {}", raw);
        EventIngestResult result = h.service().ingestRaw(raw);
        log.info("[判定依据] accepted={} rejectReason={}（期望 {}）",
                result.isAccepted(), result.getRejectReason(), expected);
        assertFalse(result.isAccepted());
        assertEquals(expected, result.getRejectReason());
        return result;
    }

    private Map<String, Object> base() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("eventId", "e1");
        m.put("pipelineName", "p");
        m.put("runId", "r");
        m.put("eventType", "SUCCESS");
        m.put("eventTime", T);
        return m;
    }

    @Test
    void rejectByDistinctReasons(@TempDir Path dir) {
        var h = harness(dir);

        reject(h, "{not-json", RejectReason.MALFORMED_JSON);

        Map<String, Object> noId = base();
        noId.remove("eventId");
        reject(h, Events.raw(noId), RejectReason.MISSING_EVENT_ID);

        Map<String, Object> noPipeline = base();
        noPipeline.remove("pipelineName");
        reject(h, Events.raw(noPipeline), RejectReason.MISSING_PIPELINE_NAME);

        Map<String, Object> noRun = base();
        noRun.remove("runId");
        reject(h, Events.raw(noRun), RejectReason.MISSING_RUN_ID);

        Map<String, Object> noType = base();
        noType.remove("eventType");
        reject(h, Events.raw(noType), RejectReason.MISSING_EVENT_TYPE);

        Map<String, Object> noTime = base();
        noTime.remove("eventTime");
        reject(h, Events.raw(noTime), RejectReason.MISSING_EVENT_TIME);

        Map<String, Object> badType = base();
        badType.put("eventType", "EXPLODED");
        reject(h, Events.raw(badType), RejectReason.UNKNOWN_EVENT_TYPE);

        Map<String, Object> badTime = base();
        badTime.put("eventTime", -5);
        reject(h, Events.raw(badTime), RejectReason.INVALID_EVENT_TIME);
    }

    @Test
    void rejectedEventsAreRecordedNotDropped(@TempDir Path dir) {
        var h = harness(dir);
        h.service().ingestRaw("{bad");
        h.service().ingestRaw(Events.json("ok", "p", "r", "STARTED", T));
        Map<String, Object> noRun = base();
        noRun.remove("runId");
        h.service().ingestRaw(Events.raw(noRun));

        var rejected = h.service().rejectedEvents();
        log.info("[判定依据] 被拒事件条数={}，原因={}", rejected.size(),
                rejected.stream().map(r -> r.getReason()).toList());
        assertEquals(2, rejected.size());
        assertEquals(List.of("MALFORMED_JSON", "MISSING_RUN_ID"),
                rejected.stream().map(r -> r.getReason()).toList());
        assertNotNull(rejected.get(0).getRawPayload());
        assertNotNull(rejected.get(0).getRejectedAt());
    }

    @Test
    void validEventAccepted(@TempDir Path dir) {
        var h = harness(dir);
        String raw = Events.json("e1", "p", "r", "SUCCESS", T);
        log.info("[输入事件] {}", raw);
        EventIngestResult result = h.service().ingestRaw(raw);
        log.info("[判定依据] accepted={} duplicate={}", result.isAccepted(), result.isDuplicate());
        assertTrue(result.isAccepted());
        assertFalse(result.isDuplicate());
        assertTrue(h.service().rejectedEvents().isEmpty());
    }
}
