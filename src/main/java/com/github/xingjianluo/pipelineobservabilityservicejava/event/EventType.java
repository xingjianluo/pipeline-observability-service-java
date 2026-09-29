package com.github.xingjianluo.pipelineobservabilityservicejava.event;

/** 流水线运行事件类型。 */
public enum EventType {
    /** 运行开始。 */
    STARTED,
    /** 失败后开始一次新的尝试（同一 runId 内）。 */
    RETRYING,
    /** 运行成功（终态）。 */
    SUCCEEDED,
    /** 运行失败（终态）。 */
    FAILED,
    /** 运行超时（终态）。 */
    TIMED_OUT;

    /** 是否为终态事件：终态后只接受更早的纠正事件。 */
    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == TIMED_OUT;
    }
}
