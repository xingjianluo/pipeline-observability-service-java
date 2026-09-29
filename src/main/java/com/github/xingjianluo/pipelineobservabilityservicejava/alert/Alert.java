package com.github.xingjianluo.pipelineobservabilityservicejava.alert;

import java.time.Instant;

/** 一条实际发出的告警记录（不可变）。 */
public record Alert(
        String pipelineName,
        String runId,
        AlertType type,
        Instant firedAt,
        String message) {
}
