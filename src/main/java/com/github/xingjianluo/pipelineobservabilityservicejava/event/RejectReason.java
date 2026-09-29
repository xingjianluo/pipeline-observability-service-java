package com.github.xingjianluo.pipelineobservabilityservicejava.event;

/** 事件被拒绝接入的不同原因，码值用于对外返回与审计。 */
public enum RejectReason {
    JSON_INVALID("请求体不是合法的 JSON"),
    MISSING_PIPELINE_NAME("缺少 pipelineName"),
    MISSING_RUN_ID("缺少 runId"),
    MISSING_EVENT_TYPE("缺少 type"),
    UNKNOWN_EVENT_TYPE("不认识的事件 type"),
    MISSING_EVENT_TIME("缺少 eventTime"),
    INVALID_EVENT_TIME("eventTime 不是合法的 ISO-8601 时间"),
    MISSING_FAILURE_DETAIL("FAILED/TIMED_OUT 事件必须带 detail 失败原因"),
    EVENT_TIME_IN_FUTURE("eventTime 不能晚于当前时间");

    private final String defaultMessage;

    RejectReason(String defaultMessage) {
        this.defaultMessage = defaultMessage;
    }

    public String getDefaultMessage() {
        return defaultMessage;
    }
}
