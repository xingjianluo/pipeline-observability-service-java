package com.github.xingjianluo.pipelineobservabilityservicejava.web.dto;

/** 事件接入请求体（宽松绑定，严格校验在 parser 里做）。 */
public class EventRequest {
    private String pipelineName;
    private String runId;
    private String type;
    private String eventTime;
    private String eventId;
    private String detail;

    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getEventTime() { return eventTime; }
    public void setEventTime(String eventTime) { this.eventTime = eventTime; }
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }
}
