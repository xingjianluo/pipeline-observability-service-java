package com.github.xingjianluo.pipelineobservabilityservicejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** 可观测服务阈值与存储配置，均可通过 application.properties 覆盖。 */
@Component
@ConfigurationProperties(prefix = "pipeline.observability")
public class ObservabilityProperties {

    /** 新鲜度阈值：距最近一次成功超过该时长视为 stale。 */
    private long freshnessThresholdMs = 3_600_000L;
    /** SLA 阈值：单次运行从 STARTED 到终态超过该时长视为延迟完成。 */
    private long slaThresholdMs = 1_800_000L;
    /** 连续失败告警阈值。 */
    private int consecutiveFailureThreshold = 3;
    /** 告警抑制窗口：同一问题在窗口内只提醒一次。 */
    private long alertSuppressionWindowMs = 1_800_000L;
    /** JSON 持久化目录。 */
    private String storageDir = "data/observability";

    public long getFreshnessThresholdMs() { return freshnessThresholdMs; }
    public void setFreshnessThresholdMs(long v) { this.freshnessThresholdMs = v; }
    public long getSlaThresholdMs() { return slaThresholdMs; }
    public void setSlaThresholdMs(long v) { this.slaThresholdMs = v; }
    public int getConsecutiveFailureThreshold() { return consecutiveFailureThreshold; }
    public void setConsecutiveFailureThreshold(int v) { this.consecutiveFailureThreshold = v; }
    public long getAlertSuppressionWindowMs() { return alertSuppressionWindowMs; }
    public void setAlertSuppressionWindowMs(long v) { this.alertSuppressionWindowMs = v; }
    public String getStorageDir() { return storageDir; }
    public void setStorageDir(String v) { this.storageDir = v; }
}
