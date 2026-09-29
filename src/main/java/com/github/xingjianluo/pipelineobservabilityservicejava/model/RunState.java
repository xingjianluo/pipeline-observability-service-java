package com.github.xingjianluo.pipelineobservabilityservicejava.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 单个运行实例（pipelineName + runId）的归并状态。
 * 事件乱序到达时按 eventTime 归并，终态由"事件类型优先级 + 时间"规则确定。
 */
public class RunState {
    private String pipelineName;
    private String runId;
    private Long startedAt;
    private Long finishedAt;
    private EventType terminalEventType;
    private RunOutcome outcome;
    private int attemptCount;
    private List<String> acceptedEventIds = new ArrayList<>();

    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public Long getStartedAt() { return startedAt; }
    public void setStartedAt(Long startedAt) { this.startedAt = startedAt; }
    public Long getFinishedAt() { return finishedAt; }
    public void setFinishedAt(Long finishedAt) { this.finishedAt = finishedAt; }
    public EventType getTerminalEventType() { return terminalEventType; }
    public void setTerminalEventType(EventType terminalEventType) { this.terminalEventType = terminalEventType; }
    public RunOutcome getOutcome() { return outcome; }
    public void setOutcome(RunOutcome outcome) { this.outcome = outcome; }
    public int getAttemptCount() { return attemptCount; }
    public void setAttemptCount(int attemptCount) { this.attemptCount = attemptCount; }
    public List<String> getAcceptedEventIds() { return acceptedEventIds; }
    public void setAcceptedEventIds(List<String> acceptedEventIds) { this.acceptedEventIds = acceptedEventIds; }
}
