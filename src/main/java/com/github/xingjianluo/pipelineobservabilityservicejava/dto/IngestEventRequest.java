package com.github.xingjianluo.pipelineobservabilityservicejava.dto;

/** 接入的单个运行事件（校验通过后的规范化对象，时间均为 epoch 毫秒）。 */
public class IngestEventRequest {
    private String eventId;
    private String pipelineName;
    private String runId;
    private String eventType;
    private Long eventTime;

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public Long getEventTime() { return eventTime; }
    public void setEventTime(Long eventTime) { this.eventTime = eventTime; }
}
