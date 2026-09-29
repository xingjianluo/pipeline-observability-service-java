package com.github.xingjianluo.pipelineobservabilityservicejava.alert;

import java.time.Instant;

/** 某条流水线某类告警的抑制状态，可变，可持久化。 */
public class AlertState {

    private AlertType type;
    private boolean active;
    private Instant lastFiredAt;
    private Instant suppressedUntil;
    private long fireCount;

    public AlertType getType() { return type; }
    public void setType(AlertType type) { this.type = type; }
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
    public Instant getLastFiredAt() { return lastFiredAt; }
    public void setLastFiredAt(Instant lastFiredAt) { this.lastFiredAt = lastFiredAt; }
    public Instant getSuppressedUntil() { return suppressedUntil; }
    public void setSuppressedUntil(Instant suppressedUntil) { this.suppressedUntil = suppressedUntil; }
    public long getFireCount() { return fireCount; }
    public void setFireCount(long fireCount) { this.fireCount = fireCount; }
}
