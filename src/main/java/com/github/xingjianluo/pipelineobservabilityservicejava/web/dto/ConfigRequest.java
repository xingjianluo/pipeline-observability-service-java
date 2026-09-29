package com.github.xingjianluo.pipelineobservabilityservicejava.web.dto;

import java.time.Duration;

/** 阈值配置请求体。 */
public class ConfigRequest {
    private String pipelineName;
    private Duration freshnessThreshold;
    private Duration sla;
    private Integer failureAlertThreshold;
    private Duration alertSuppressWindow;

    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public Duration getFreshnessThreshold() { return freshnessThreshold; }
    public void setFreshnessThreshold(Duration freshnessThreshold) { this.freshnessThreshold = freshnessThreshold; }
    public Duration getSla() { return sla; }
    public void setSla(Duration sla) { this.sla = sla; }
    public Integer getFailureAlertThreshold() { return failureAlertThreshold; }
    public void setFailureAlertThreshold(Integer failureAlertThreshold) { this.failureAlertThreshold = failureAlertThreshold; }
    public Duration getAlertSuppressWindow() { return alertSuppressWindow; }
    public void setAlertSuppressWindow(Duration alertSuppressWindow) { this.alertSuppressWindow = alertSuppressWindow; }
}
