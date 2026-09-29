package com.github.xingjianluo.pipelineobservabilityservicejava.alert;

import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineConfig;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineStatusSnapshot;

import java.time.Instant;
import java.util.List;

/**
 * 根据状态快照决定告警是否发出：抑制窗口内同类只发一次，
 * 恢复后重置并允许再次触发。
 */
public interface AlertService {

    /**
     * 评估快照中的所有告警条件。
     * @param config 该流水线的阈值（取抑制窗口）
     * @return 本次实际新发出的告警（被抑制的不在其中）
     */
    List<Alert> evaluate(PipelineStatusSnapshot snapshot, PipelineConfig config, Instant now);

    /**
     * 时间巡检评估：事件刚接入时活动正在发生，不可能"运行中超时"，
     * 该路径下跳过运行中 SLA（runningPastSla）判定；延迟完成的 SLA 不受影响。
     */
    List<Alert> evaluate(PipelineStatusSnapshot snapshot, PipelineConfig config,
                         Instant now, boolean timeBasedProbe);

    /** 某流水线当前仍活跃（未恢复）的告警类型。 */
    List<AlertType> activeAlerts(String pipelineName);

    /** 已发出的告警历史。 */
    List<Alert> history(String pipelineName);
}
