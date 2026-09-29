package com.github.xingjianluo.pipelineobservabilityservicejava.event;

import com.github.xingjianluo.pipelineobservabilityservicejava.dto.IngestEventRequest;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.RejectReason;

/**
 * 事件校验结果。
 * reason == null 表示通过；拒绝时 request 仍尽力携带可解析字段，便于留痕。
 */
public record ValidationResult(IngestEventRequest request, RejectReason reason) {

    public boolean isValid() {
        return reason == null;
    }

    public static ValidationResult valid(IngestEventRequest request) {
        return new ValidationResult(request, null);
    }

    public static ValidationResult invalid(IngestEventRequest partial, RejectReason reason) {
        return new ValidationResult(partial, reason);
    }
}
