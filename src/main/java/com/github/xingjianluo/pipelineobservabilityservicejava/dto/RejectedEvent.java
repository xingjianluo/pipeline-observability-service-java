package com.github.xingjianluo.pipelineobservabilityservicejava.dto;

/** 被拒绝接入的事件记录：不静默丢弃，按原因留痕，事后可查。 */
public class RejectedEvent {
    private String eventId;
    private String pipelineName;
    private String runId;
    private String rawEventType;
    private Long rawEventTime;
    private String reason;
    private String rawPayload;
    private Long rejectedAt;

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public String getRawEventType() { return rawEventType; }
    public void setRawEventType(String rawEventType) { this.rawEventType = rawEventType; }
    public Long getRawEventTime() { return rawEventTime; }
    public void setRawEventTime(Long rawEventTime) { this.rawEventTime = rawEventTime; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
    public String getRawPayload() { return rawPayload; }
    public void setRawPayload(String rawPayload) { this.rawPayload = rawPayload; }
    public Long getRejectedAt() { return rejectedAt; }
    public void setRejectedAt(Long rejectedAt) { this.rejectedAt = rejectedAt; }
}
