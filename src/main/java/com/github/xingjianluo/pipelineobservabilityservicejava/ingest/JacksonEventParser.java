package com.github.xingjianluo.pipelineobservabilityservicejava.ingest;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.EventType;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RejectReason;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RejectedEventException;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RunEvent;
import com.github.xingjianluo.pipelineobservabilityservicejava.web.dto.EventRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * 用 Jackson 解析事件，然后按固定顺序严格校验。
 * 任何校验失败都以明确的 {@link RejectReason} 拒绝（不丢弃、不含糊）。
 */
@Component
public class JacksonEventParser implements EventParser {

    private static final Logger log = LoggerFactory.getLogger(JacksonEventParser.class);

    private final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static String blankToNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    @Override
    public RunEvent parse(String rawPayload, Instant now) throws RejectedEventException {
        EventRequest req;
        try {
            req = mapper.readValue(rawPayload, EventRequest.class);
        } catch (Exception e) {
            log.warn("事件被拒绝(JSON_INVALID): {}", rawPayload);
            throw new RejectedEventException(RejectReason.JSON_INVALID,
                    RejectReason.JSON_INVALID.getDefaultMessage() + ": " + e.getMessage(),
                    rawPayload);
        }

        String pipelineName = blankToNull(req.getPipelineName());
        if (pipelineName == null) {
            throw reject(RejectReason.MISSING_PIPELINE_NAME, rawPayload);
        }
        String runId = blankToNull(req.getRunId());
        if (runId == null) {
            throw reject(RejectReason.MISSING_RUN_ID, rawPayload);
        }
        String typeRaw = blankToNull(req.getType());
        if (typeRaw == null) {
            throw reject(RejectReason.MISSING_EVENT_TYPE, rawPayload);
        }
        EventType type;
        try {
            type = EventType.valueOf(typeRaw.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw reject(RejectReason.UNKNOWN_EVENT_TYPE, typeRaw, rawPayload);
        }
        String timeRaw = blankToNull(req.getEventTime());
        if (timeRaw == null) {
            throw reject(RejectReason.MISSING_EVENT_TIME, rawPayload);
        }
        Instant eventTime;
        try {
            eventTime = Instant.parse(timeRaw);
        } catch (DateTimeParseException e) {
            throw reject(RejectReason.INVALID_EVENT_TIME, timeRaw, rawPayload);
        }
        if (eventTime.isAfter(now)) {
            throw reject(RejectReason.EVENT_TIME_IN_FUTURE,
                    "eventTime=" + eventTime + " 晚于当前时间 " + now, rawPayload);
        }
        String detail = blankToNull(req.getDetail());
        if ((type == EventType.FAILED || type == EventType.TIMED_OUT) && detail == null) {
            throw reject(RejectReason.MISSING_FAILURE_DETAIL, rawPayload);
        }
        String eventId = blankToNull(req.getEventId());

        return new RunEvent(pipelineName, runId, type, eventTime, eventId, detail, now);
    }

    private RejectedEventException reject(RejectReason reason, String raw) {
        return reject(reason, reason.getDefaultMessage(), raw);
    }

    private RejectedEventException reject(RejectReason reason, String message, String raw) {
        log.warn("事件被拒绝({}): {} | payload={}", reason, message, raw);
        return new RejectedEventException(reason, message, raw);
    }
}
