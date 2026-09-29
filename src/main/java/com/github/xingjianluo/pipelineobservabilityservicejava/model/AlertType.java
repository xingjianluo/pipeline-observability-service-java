package com.github.xingjianluo.pipelineobservabilityservicejava.model;

/** 告警类别，三类问题严格区分，不合并为笼统失败。 */
public enum AlertType {
    /** 连续失败次数达到阈值。 */
    CONSECUTIVE_FAILURES,
    /** 一次运行从开始到终态耗时超过 SLA。 */
    DELAYED_COMPLETION,
    /** 距最近一次成功超过新鲜度阈值。 */
    STALE_NO_SUCCESS
}
