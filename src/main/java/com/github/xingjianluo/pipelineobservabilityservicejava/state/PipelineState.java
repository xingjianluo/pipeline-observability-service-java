package com.github.xingjianluo.pipelineobservabilityservicejava.state;

import com.github.xingjianluo.pipelineobservabilityservicejava.run.RunState;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 一条流水线的全部聚合状态，可变，整体持久化。
 */
public class PipelineState {

    private String pipelineName;
    private Instant lastSuccessAt;
    private long consecutiveFailures;
    private long totalSuccesses;
    private long totalFailures;
    private Map<String, RunState> runs = new LinkedHashMap<>();
    private long version;
    private Instant updatedAt;

    public String getPipelineName() { return pipelineName; }
    public void setPipelineName(String pipelineName) { this.pipelineName = pipelineName; }
    public Instant getLastSuccessAt() { return lastSuccessAt; }
    public void setLastSuccessAt(Instant lastSuccessAt) { this.lastSuccessAt = lastSuccessAt; }
    public long getConsecutiveFailures() { return consecutiveFailures; }
    public void setConsecutiveFailures(long consecutiveFailures) { this.consecutiveFailures = consecutiveFailures; }
    public long getTotalSuccesses() { return totalSuccesses; }
    public void setTotalSuccesses(long totalSuccesses) { this.totalSuccesses = totalSuccesses; }
    public long getTotalFailures() { return totalFailures; }
    public void setTotalFailures(long totalFailures) { this.totalFailures = totalFailures; }
    public Map<String, RunState> getRuns() { return runs; }
    public void setRuns(Map<String, RunState> runs) { this.runs = runs; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
