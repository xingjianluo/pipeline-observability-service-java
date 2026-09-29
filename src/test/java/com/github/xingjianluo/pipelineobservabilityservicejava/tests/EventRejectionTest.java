package com.github.xingjianluo.pipelineobservabilityservicejava.tests;

import com.github.xingjianluo.pipelineobservabilityservicejava.event.RejectReason;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RejectedEventException;
import com.github.xingjianluo.pipelineobservabilityservicejava.ingest.JacksonEventParser;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 失败/缺字段事件必须按不同原因拒绝，不能笼统丢弃。 */
class EventRejectionTest {

    private static final Logger log = LoggerFactory.getLogger(EventRejectionTest.class);
    private final JacksonEventParser parser = new JacksonEventParser();
    private final Instant now = Instant.parse("2026-09-29T12:00:00Z");

    private void assertRejected(String label, String payload, RejectReason expected) {
        log.info(">>> 拒绝场景[{}] payload={}", label, payload);
        try {
            parser.parse(payload, now);
            throw new AssertionError("应当被拒绝: " + label);
        } catch (RejectedEventException e) {
            log.info("    拒绝原因={} message={}", e.getReason(), e.getMessage());
            assertEquals(expected, e.getReason());
        }
    }

    @Test
    void rejectsByDistinctReasons() {
        assertRejected("非法JSON", "{not-json", RejectReason.JSON_INVALID);
        assertRejected("缺pipelineName",
                "{\"runId\":\"r1\",\"type\":\"STARTED\",\"eventTime\":\"2026-09-29T11:00:00Z\"}",
                RejectReason.MISSING_PIPELINE_NAME);
        assertRejected("缺runId",
                "{\"pipelineName\":\"p\",\"type\":\"STARTED\",\"eventTime\":\"2026-09-29T11:00:00Z\"}",
                RejectReason.MISSING_RUN_ID);
        assertRejected("缺type",
                "{\"pipelineName\":\"p\",\"runId\":\"r1\",\"eventTime\":\"2026-09-29T11:00:00Z\"}",
                RejectReason.MISSING_EVENT_TYPE);
        assertRejected("未知type",
                "{\"pipelineName\":\"p\",\"runId\":\"r1\",\"type\":\"EXPLODED\",\"eventTime\":\"2026-09-29T11:00:00Z\"}",
                RejectReason.UNKNOWN_EVENT_TYPE);
        assertRejected("缺eventTime",
                "{\"pipelineName\":\"p\",\"runId\":\"r1\",\"type\":\"STARTED\"}",
                RejectReason.MISSING_EVENT_TIME);
        assertRejected("非法时间",
                "{\"pipelineName\":\"p\",\"runId\":\"r1\",\"type\":\"STARTED\",\"eventTime\":\"tomorrow\"}",
                RejectReason.INVALID_EVENT_TIME);
        assertRejected("未来时间",
                "{\"pipelineName\":\"p\",\"runId\":\"r1\",\"type\":\"STARTED\",\"eventTime\":\"2026-09-30T12:00:00Z\"}",
                RejectReason.EVENT_TIME_IN_FUTURE);
        assertRejected("失败缺detail",
                "{\"pipelineName\":\"p\",\"runId\":\"r1\",\"type\":\"FAILED\",\"eventTime\":\"2026-09-29T11:00:00Z\"}",
                RejectReason.MISSING_FAILURE_DETAIL);
        assertRejected("超时缺detail",
                "{\"pipelineName\":\"p\",\"runId\":\"r1\",\"type\":\"TIMED_OUT\",\"eventTime\":\"2026-09-29T11:00:00Z\"}",
                RejectReason.MISSING_FAILURE_DETAIL);
    }

    @Test
    void acceptsValidEvent() {
        String payload = "{\"pipelineName\":\"p\",\"runId\":\"r1\",\"type\":\"FAILED\","
                + "\"eventTime\":\"2026-09-29T11:00:00Z\",\"detail\":\"oom\"}";
        log.info(">>> 正常事件 payload={}", payload);
        var e = parser.parse(payload, now);
        log.info("    解析成功 pipeline={} run={} type={} detail={}",
                e.pipelineName(), e.runId(), e.type(), e.detail());
        assertEquals("p", e.pipelineName());
        assertEquals(Instant.parse("2026-09-29T11:00:00Z"), e.eventTime());
    }
}
