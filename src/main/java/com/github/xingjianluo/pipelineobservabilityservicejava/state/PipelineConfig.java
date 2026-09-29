package com.github.xingjianluo.pipelineobservabilityservicejava.state;

import java.time.Duration;

/** 流水线阈值配置（可在运行中更新）。 */
public class PipelineConfig {

    private String pipelineName;
    private Duration freshnessThreshold;
    private Duration sla;
    private int failureAlertThreshold;
    private Duration alertSuppressWindow;

    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public Duration getFreshnessThreshold() { return freshnessThreshold; }
    public void setFreshnessThreshold(Duration freshnessThreshold) { this.freshnessThreshold = freshnessThreshold; }
    public Duration getSla() { return sla; }
    public void setSla(Duration sla) { this.sla = sla; }
    public int getFailureAlertThreshold() { return failureAlertThreshold; }
    public void setFailureAlertThreshold(int failureAlertThreshold) { this.failureAlertThreshold = failureAlertThreshold; }
    public Duration getAlertSuppressWindow() { return alertSuppressWindow; }
    public void setAlertSuppressWindow(Duration alertSuppressWindow) { this.alertSuppressWindow = alertSuppressWindow; }
}
