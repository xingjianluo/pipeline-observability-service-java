package com.github.xingjianluo.pipelineobservabilityservicejava.web.dto;

/** 事件被拒绝时的结构化响应。 */
public record RejectResponse(String rejectedReason, String message) {
}
