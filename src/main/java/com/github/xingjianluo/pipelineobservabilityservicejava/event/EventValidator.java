package com.github.xingjianluo.pipelineobservabilityservicejava.event;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityservicejava.dto.IngestEventRequest;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.EventType;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.RejectReason;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 事件字段校验：失败按不同原因拒绝，不静默丢弃。
 *
 * <p>校验顺序固定，便于事后排查：JSON 语法 → eventId → pipelineName → runId
 * → eventType(存在) → eventTime(存在) → eventType(合法) → eventTime(合法)。</p>
 */
@Component
public class EventValidator {

    private final ObjectMapper objectMapper;

    public EventValidator() {
        this.objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /** 解析并校验原始 JSON；解析失败返回 MALFORMED_JSON（partial 为空对象，留痕原始载荷）。 */
    public ValidationResult validateRaw(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            return ValidationResult.invalid(new IngestEventRequest(), RejectReason.MALFORMED_JSON);
        }
        JsonNode node;
        try {
            node = objectMapper.readTree(rawJson);
        } catch (IOException e) {
            return ValidationResult.invalid(new IngestEventRequest(), RejectReason.MALFORMED_JSON);
        }
        if (node == null || !node.isObject()) {
            return ValidationResult.invalid(new IngestEventRequest(), RejectReason.MALFORMED_JSON);
        }
        return validateNode(node);
    }

    private ValidationResult validateNode(JsonNode node) {
        IngestEventRequest partial = new IngestEventRequest();
        putText(partial::setEventId, node, "eventId");
        putText(partial::setPipelineName, node, "pipelineName");
        putText(partial::setRunId, node, "runId");
        partial.setEventType(text(node, "eventType"));
        if (node.has("eventTime") && node.get("eventTime").canConvertToLong()) {
            partial.setEventTime(node.get("eventTime").asLong());
        }

        if (isBlank(partial.getEventId())) {
            return ValidationResult.invalid(partial, RejectReason.MISSING_EVENT_ID);
        }
        if (isBlank(partial.getPipelineName())) {
            return ValidationResult.invalid(partial, RejectReason.MISSING_PIPELINE_NAME);
        }
        if (isBlank(partial.getRunId())) {
            return ValidationResult.invalid(partial, RejectReason.MISSING_RUN_ID);
        }
        if (isBlank(partial.getEventType())) {
            return ValidationResult.invalid(partial, RejectReason.MISSING_EVENT_TYPE);
        }
        if (!node.has("eventTime") || node.get("eventTime").isNull()) {
            return ValidationResult.invalid(partial, RejectReason.MISSING_EVENT_TIME);
        }
        // eventTime 存在但不是数字（字符串/对象/布尔/数组等）→ 缺失类与非法类分开报告
        if (!node.get("eventTime").canConvertToLong()) {
            return ValidationResult.invalid(partial, RejectReason.MISSING_EVENT_TIME);
        }
        EventType parsedType;
        try {
            parsedType = EventType.valueOf(partial.getEventType().trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return ValidationResult.invalid(partial, RejectReason.UNKNOWN_EVENT_TYPE);
        }
        long eventTime = partial.getEventTime();
        if (eventTime <= 0L) {
            return ValidationResult.invalid(partial, RejectReason.INVALID_EVENT_TIME);
        }
        partial.setEventType(parsedType.name());
        return ValidationResult.valid(partial);
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v != null && v.isTextual() ? v.asText() : null;
    }

    private static void putText(java.util.function.Consumer<String> setter, JsonNode node, String field) {
        String v = text(node, field);
        if (v != null) {
            setter.accept(v);
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
