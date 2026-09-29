package com.github.xingjianluo.pipelineobservabilityservicejava.support;

import com.github.xingjianluo.pipelineobservabilityservicejava.alert.AlertSink;
import com.github.xingjianluo.pipelineobservabilityservicejava.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.EventValidator;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RunEventApplier;
import com.github.xingjianluo.pipelineobservabilityservicejava.service.AlertEvaluator;
import com.github.xingjianluo.pipelineobservabilityservicejava.service.PipelineObservabilityService;
import com.github.xingjianluo.pipelineobservabilityservicejava.service.StatusCalculator;
import com.github.xingjianluo.pipelineobservabilityservicejava.store.JsonFilePipelineStateStore;

/** 测试装配：固定小阈值 + 可控时钟 + 临时目录 + 收集型告警出口。 */
public final class TestServiceFactory {

    /** 装配结果容器。 */
    public record Harness(PipelineObservabilityService service,
                          MutableClock clock,
                          CollectingAlertSink alerts,
                          JsonFilePipelineStateStore store,
                          ObservabilityProperties props,
                          AlertEvaluator evaluator) {}

    private TestServiceFactory() {}

    /** 固定阈值：SLA=10min，新鲜度=1h，连续失败阈值=3，抑制窗口=30min。 */
    public static Harness create(String storageDir, MutableClock clock, CollectingAlertSink alerts) {
        ObservabilityProperties props = new ObservabilityProperties();
        props.setStorageDir(storageDir);
        props.setSlaThresholdMs(10 * 60_000L);
        props.setFreshnessThresholdMs(60 * 60_000L);
        props.setConsecutiveFailureThreshold(3);
        props.setAlertSuppressionWindowMs(30 * 60_000L);

        JsonFilePipelineStateStore store = new JsonFilePipelineStateStore(props);
        try {
            store.init();
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
        StatusCalculator calculator = new StatusCalculator(props, clock);
        AlertEvaluator evaluator = new AlertEvaluator(props, calculator, clock, alerts);
        EventValidator validator = new EventValidator();
        RunEventApplier applier = new RunEventApplier();
        PipelineObservabilityService service =
                new PipelineObservabilityService(validator, applier, store, calculator, evaluator, clock);
        return new Harness(service, clock, alerts, store, props, evaluator);
    }

    /** 模拟重启：同一持久化目录新建一套组件（内存为空，全部从磁盘恢复）。 */
    public static Harness restart(Harness previous, MutableClock clock, CollectingAlertSink alerts) {
        return create(previous.props().getStorageDir(), clock, alerts);
    }
}
