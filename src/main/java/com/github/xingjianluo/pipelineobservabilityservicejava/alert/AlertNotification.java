package com.github.xingjianluo.pipelineobservabilityservicejava.alert;

/** 实际发出的一条告警通知。 */
public record AlertNotification(
        String type,
        String pipelineName,
        String runId,
        long firedAt,
        String message
) {}
