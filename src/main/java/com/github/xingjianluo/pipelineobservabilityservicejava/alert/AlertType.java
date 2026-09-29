package com.github.xingjianluo.pipelineobservabilityservicejava.alert;

/** 告警类型，三种问题严格区分。 */
public enum AlertType {
    /** 连续失败次数达到阈值。 */
    CONSECUTIVE_FAILURES,
    /** 运行完成但耗时超过 SLA（或运行中已超 SLA）。 */
    SLA_BREACHED,
    /** 距最近一次成功超过新鲜度阈值，长时间没有成功。 */
    STALE_NO_SUCCESS
}
