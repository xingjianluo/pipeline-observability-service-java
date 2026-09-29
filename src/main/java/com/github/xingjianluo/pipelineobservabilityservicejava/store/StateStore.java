package com.github.xingjianluo.pipelineobservabilityservicejava.store;

import com.github.xingjianluo.pipelineobservabilityservicejava.alert.Alert;
import com.github.xingjianluo.pipelineobservabilityservicejava.alert.AlertState;
import com.github.xingjianluo.pipelineobservabilityservicejava.audit.DecisionRecord;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineConfig;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineState;

import java.util.List;
import java.util.Map;

/**
 * 状态持久化存储：流水线聚合状态、告警抑制状态、配置、判定记录都要落盘，
 * 保证进程重启后结论一致。
 */
public interface StateStore {

    Map<String, PipelineState> loadPipelineStates();

    void savePipelineState(PipelineState state);

    Map<String, Map<com.github.xingjianluo.pipelineobservabilityservicejava.alert.AlertType, AlertState>> loadAlertStates();

    void saveAlertState(String pipelineName, AlertState alertState);

    /** 已发出的全部告警历史。 */
    List<Alert> loadAlerts();

    void appendAlert(Alert alert);

    Map<String, PipelineConfig> loadConfigs();

    void saveConfig(PipelineConfig config);

    List<DecisionRecord> loadDecisions();

    void appendDecision(DecisionRecord record);
}
