package com.github.xingjianluo.pipelineobservabilityservicejava.service;

import com.github.xingjianluo.pipelineobservabilityservicejava.store.PipelineStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 定时巡检：逐流水线做时间驱动评估。
 * 只负责产生 STALE_NO_SUCCESS 这类"没有新事件、纯随时间流逝才暴露"的告警；
 * 连续失败/延迟完成在事件接入时已即时评估。
 */
@Component
public class StaleAlertScheduler {

    private static final Logger log = LoggerFactory.getLogger(StaleAlertScheduler.class);

    private final PipelineStateStore store;
    private final AlertEvaluator alertEvaluator;

    public StaleAlertScheduler(PipelineStateStore store, AlertEvaluator alertEvaluator) {
        this.store = store;
        this.alertEvaluator = alertEvaluator;
    }

    @Scheduled(fixedDelayString = "${pipeline.observability.scan-interval-ms:60000}",
            initialDelayString = "${pipeline.observability.scan-initial-delay-ms:10000}")
    public void scan() {
        for (String name : store.findAllNames()) {
            try {
                // 在 pipeline 锁内评估，告警状态变更随 compute 原子落盘，不与事件写竞争
                store.compute(name, state -> {
                    alertEvaluator.evaluateByTime(state);
                    return state;
                });
            } catch (Exception e) {
                log.error("Stale scan failed for pipeline {}", name, e);
            }
        }
    }
}
