package com.github.xingjianluo.pipelineobservabilityservicejava.event;

import com.github.xingjianluo.pipelineobservabilityservicejava.model.RejectReason;

/** 单条事件接入结果。duplicate=true 也算"已被系统接受过"（幂等成功），不算失败。 */
public class EventIngestResult {
    private final boolean accepted;
    private final boolean duplicate;
    private final RejectReason rejectReason;
    private final String message;

    private EventIngestResult(boolean accepted, boolean duplicate, RejectReason rejectReason, String message) {
        this.accepted = accepted;
        this.duplicate = duplicate;
        this.rejectReason = rejectReason;
        this.message = message;
    }

    public static EventIngestResult accepted() {
        return new EventIngestResult(true, false, null, "event accepted");
    }

    public static EventIngestResult duplicate(String eventId) {
        return new EventIngestResult(true, true, null, "duplicate event ignored idempotently: " + eventId);
    }

    public static EventIngestResult rejected(RejectReason reason) {
        return new EventIngestResult(false, false, reason, "event rejected: " + reason.name());
    }

    public boolean isAccepted() { return accepted; }
    public boolean isDuplicate() { return duplicate; }
    public RejectReason getRejectReason() { return rejectReason; }
    public String getMessage() { return message; }
}
