package com.github.xingjianluo.pipelineobservabilityservicejava.run;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 单个运行实例（runId）的推算状态，可变，可持久化。
 */
public class RunState {

    private String runId;
    private RunStatus status;
    private Instant startedAt;
    private Instant endedAt;
    private Instant lastEventTime;
    private int attempts;
    private String lastError;
    private boolean terminal;
    private List<String> seenEventIds = new ArrayList<>();
    private Instant updatedAt;

    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public RunStatus getStatus() { return status; }
    public void setStatus(RunStatus status) { this.status = status; }
    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public Instant getEndedAt() { return endedAt; }
    public void setEndedAt(Instant endedAt) { this.endedAt = endedAt; }
    public Instant getLastEventTime() { return lastEventTime; }
    public void setLastEventTime(Instant lastEventTime) { this.lastEventTime = lastEventTime; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
    public boolean isTerminal() { return terminal; }
    public void setTerminal(boolean terminal) { this.terminal = terminal; }
    public List<String> getSeenEventIds() { return seenEventIds; }
    public void setSeenEventIds(List<String> seenEventIds) { this.seenEventIds = seenEventIds; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
