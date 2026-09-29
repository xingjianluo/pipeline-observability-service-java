package com.github.xingjianluo.pipelineobservabilityservicejava.model;

/** 流水线运行事件类型。 */
public enum EventType {
    STARTED,
    SUCCESS,
    FAILED,
    RETRIED,
    TIMED_OUT
}
