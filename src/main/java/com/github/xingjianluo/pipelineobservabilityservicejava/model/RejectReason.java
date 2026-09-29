package com.github.xingjianluo.pipelineobservabilityservicejava.model;

/** 事件被拒绝接入的原因，按原因区分，不静默丢弃。 */
public enum RejectReason {
    MALFORMED_JSON,
    MISSING_EVENT_ID,
    MISSING_PIPELINE_NAME,
    MISSING_RUN_ID,
    MISSING_EVENT_TYPE,
    MISSING_EVENT_TIME,
    UNKNOWN_EVENT_TYPE,
    INVALID_EVENT_TIME
}
