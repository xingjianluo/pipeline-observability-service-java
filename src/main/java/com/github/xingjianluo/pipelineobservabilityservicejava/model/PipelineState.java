package com.github.xingjianluo.pipelineobservabilityservicejava.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 一条流水线的全部持久化状态：运行实例、已接入事件、告警抑制状态。 */
public class PipelineState {
    private String pipelineName;
    private Map<String, RunState> runs = new LinkedHashMap<>();
    private List<String> acceptedEventIds = new ArrayList<>();
    private Map<AlertType, AlertState> alerts = new LinkedHashMap<>();

    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public Map<String, RunState> getRuns() { return runs; }
    public void setRuns(Map<String, RunState> runs) { this.runs = runs; }
    public List<String> getAcceptedEventIds() { return acceptedEventIds; }
    public void setAcceptedEventIds(List<String> acceptedEventIds) { this.acceptedEventIds = acceptedEventIds; }
    public Map<AlertType, AlertState> getAlerts() { return alerts; }
    public void setAlerts(Map<AlertType, AlertState> alerts) { this.alerts = alerts; }
}
