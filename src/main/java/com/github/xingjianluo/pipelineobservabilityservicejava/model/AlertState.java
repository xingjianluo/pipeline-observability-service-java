package com.github.xingjianluo.pipelineobservabilityservicejava.model;

/**
 * 某条流水线上某类告警的抑制状态。
 * active=true 表示问题仍在持续（抑制窗口内不再重复提醒）；
 * active=false 表示问题已恢复，下一次再触发可重新提醒。
 */
public class AlertState {
    private AlertType type;
    /** 告警作用域键：CONSECUTIVE_FAILURES/STALE_NO_SUCCESS 为 pipelineName，DELAYED_COMPLETION 为 runId。 */
    private String scopeKey;
    private boolean active;
    private Long firstFiredAt;
    private Long lastFiredAt;
    private int fireCount;
    private Long recoveredAt;

    public AlertType getType() { return type; }
    public void setType(AlertType type) { this.type = type; }
    public String getScopeKey() { return scopeKey; }
    public void setScopeKey(String scopeKey) { this.scopeKey = scopeKey; }
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
    public Long getFirstFiredAt() { return firstFiredAt; }
    public void setFirstFiredAt(Long firstFiredAt) { this.firstFiredAt = firstFiredAt; }
    public Long getLastFiredAt() { return lastFiredAt; }
    public void setLastFiredAt(Long lastFiredAt) { this.lastFiredAt = lastFiredAt; }
    public int getFireCount() { return fireCount; }
    public void setFireCount(int fireCount) { this.fireCount = fireCount; }
    public Long getRecoveredAt() { return recoveredAt; }
    public void setRecoveredAt(Long recoveredAt) { this.recoveredAt = recoveredAt; }
}
