package com.github.xingjianluo.pipelineobservabilityservicejava.support;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/** 测试用事件 JSON 构造工具。 */
public final class Events {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Events() {}

    public static String json(String eventId, String pipeline, String runId, String type, long eventTime) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("eventId", eventId);
        map.put("pipelineName", pipeline);
        map.put("runId", runId);
        map.put("eventType", type);
        map.put("eventTime", eventTime);
        try {
            return MAPPER.writeValueAsString(map);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 缺字段的事件，用于拒绝原因测试。 */
    public static String raw(Map<String, Object> fields) {
        try {
            return MAPPER.writeValueAsString(fields);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
