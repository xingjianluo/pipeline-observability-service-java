package com.github.xingjianluo.pipelineobservabilityservicejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** 服务可配置项（application.properties 中以 observability.* 覆盖）。 */
@ConfigurationProperties(prefix = "observability")
public class ObservabilityProperties {

    /** 状态文件目录。 */
    private String storageDir = "data/observability";
    /** 默认新鲜度阈值。 */
    private Duration defaultFreshnessThreshold = Duration.ofMinutes(30);
    /** 默认 SLA。 */
    private Duration defaultSla = Duration.ofMinutes(60);
    /** 默认连续失败告警阈值。 */
    private int defaultFailureAlertThreshold = 3;
    /** 默认告警抑制窗口。 */
    private Duration defaultSuppressWindow = Duration.ofMinutes(15);
    /** 是否开启后台周期性新鲜度巡检。 */
    private boolean monitorEnabled = true;
    /** 后台巡检周期。 */
    private Duration monitorInterval = Duration.ofSeconds(30);

    public String getStorageDir() { return storageDir; }
    public void setStorageDir(String storageDir) { this.storageDir = storageDir; }
    public Duration getDefaultFreshnessThreshold() { return defaultFreshnessThreshold; }
    public void setDefaultFreshnessThreshold(Duration v) { this.defaultFreshnessThreshold = v; }
    public Duration getDefaultSla() { return defaultSla; }
    public void setDefaultSla(Duration v) { this.defaultSla = v; }
    public int getDefaultFailureAlertThreshold() { return defaultFailureAlertThreshold; }
    public void setDefaultFailureAlertThreshold(int v) { this.defaultFailureAlertThreshold = v; }
    public Duration getDefaultSuppressWindow() { return defaultSuppressWindow; }
    public void setDefaultSuppressWindow(Duration v) { this.defaultSuppressWindow = v; }
    public boolean isMonitorEnabled() { return monitorEnabled; }
    public void setMonitorEnabled(boolean v) { this.monitorEnabled = v; }
    public Duration getMonitorInterval() { return monitorInterval; }
    public void setMonitorInterval(Duration v) { this.monitorInterval = v; }
}
