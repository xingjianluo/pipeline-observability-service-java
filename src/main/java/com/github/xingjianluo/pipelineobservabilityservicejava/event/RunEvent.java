package com.github.xingjianluo.pipelineobservabilityservicejava.event;

import java.time.Instant;

/**
 * 通过校验的运行事件（不可变）。
 *
 * @param eventId 调用方提供的去重标识，可为空（为空时用内容指纹去重）
 */
public record RunEvent(
        String pipelineName,
        String runId,
        EventType type,
        Instant eventTime,
        String eventId,
        String detail,
        Instant receivedAt) {
}
