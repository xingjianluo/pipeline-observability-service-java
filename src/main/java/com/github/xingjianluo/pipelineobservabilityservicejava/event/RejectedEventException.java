package com.github.xingjianluo.pipelineobservabilityservicejava.event;

/** 事件校验失败时抛出，必须带明确的拒绝原因。 */
public class RejectedEventException extends RuntimeException {

    private final RejectReason reason;
    private final String rawPayload;

    public RejectedEventException(RejectReason reason) {
        this(reason, reason.getDefaultMessage(), null);
    }

    public RejectedEventException(RejectReason reason, String message, String rawPayload) {
        super(message);
        this.reason = reason;
        this.rawPayload = rawPayload;
    }

    public RejectReason getReason() {
        return reason;
    }

    public String getRawPayload() {
        return rawPayload;
    }
}
