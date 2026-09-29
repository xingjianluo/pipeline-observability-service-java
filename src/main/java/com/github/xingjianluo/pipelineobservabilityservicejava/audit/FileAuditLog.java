package com.github.xingjianluo.pipelineobservabilityservicejava.audit;

import com.github.xingjianluo.pipelineobservabilityservicejava.store.StateStore;
import org.springframework.stereotype.Component;

import java.util.List;

/** 通过 StateStore 追加/读取判定记录。 */
@Component
public class FileAuditLog implements AuditLog {

    private final StateStore store;

    public FileAuditLog(StateStore store) {
        this.store = store;
    }

    @Override
    public void append(DecisionRecord record) {
        store.appendDecision(record);
    }

    @Override
    public List<DecisionRecord> findByPipeline(String pipelineName) {
        return store.loadDecisions().stream()
                .filter(d -> pipelineName.equals(d.pipelineName()))
                .toList();
    }

    @Override
    public List<DecisionRecord> findByRun(String pipelineName, String runId) {
        return store.loadDecisions().stream()
                .filter(d -> pipelineName.equals(d.pipelineName()) && runId.equals(d.runId()))
                .toList();
    }

    @Override
    public List<DecisionRecord> findAll() {
        return List.copyOf(store.loadDecisions());
    }
}
