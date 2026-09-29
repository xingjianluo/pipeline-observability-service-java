package com.github.xingjianluo.pipelineobservabilityservicejava.service;

import com.github.xingjianluo.pipelineobservabilityservicejava.alert.Alert;
import com.github.xingjianluo.pipelineobservabilityservicejava.audit.DecisionRecord;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RunEvent;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineConfig;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineStatusSnapshot;

import java.time.Instant;
import java.util.List;

/**
 * 对外门面：解析 -> 推算 -> 告警 -> 持久化 -> 审计，一条流水线一把锁，
 * 并发写同一条流水线不会丢更新。
 */
public interface ObservabilityService {

    /** 接入一个原始事件，返回审计记录。 */
    DecisionRecord ingest(String rawPayload, Instant now);

    /** 接入一个已解析事件（测试用）。 */
    DecisionRecord ingest(RunEvent event, Instant now);

    /** 评估某条流水线当前状态。 */
    PipelineStatusSnapshot status(String pipelineName, Instant now);

    /** 仅评估，不触发告警（后台巡检/查询用）。 */
    void evaluateTimeBasedAlerts(Instant now);

    /** 更新/新建流水线阈值配置。 */
    PipelineConfig configure(PipelineConfig config);

    /** 查判定记录。 */
    List<DecisionRecord> decisions(String pipelineName);

    /** 查告警历史。 */
    List<Alert> alerts(String pipelineName);
}
