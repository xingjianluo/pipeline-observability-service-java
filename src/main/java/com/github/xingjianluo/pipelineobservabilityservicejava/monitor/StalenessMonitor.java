package com.github.xingjianluo.pipelineobservabilityservicejava.monitor;

import com.github.xingjianluo.pipelineobservabilityservicejava.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityservicejava.service.ObservabilityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;

/**
 * 后台巡检：即使没有新事件进来，时间流逝也可能让流水线变 stale 或运行超时，
 * 周期性用当前时间评估一次。测试可通过 observability.monitor-enabled=false 关闭。
 */
@Component
public class StalenessMonitor {

    private static final Logger log = LoggerFactory.getLogger(StalenessMonitor.class);

    private final ObservabilityService service;
    private final ObservabilityProperties props;
    private final Clock clock;

    public StalenessMonitor(ObservabilityService service,
                            ObservabilityProperties props,
                            Clock clock) {
        this.service = service;
        this.props = props;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${observability.monitor-interval:30s}")
    public void tick() {
        if (!props.isMonitorEnabled()) {
            return;
        }
        Instant now = clock.instant();
        log.debug("巡检 @ {}", now);
        service.evaluateTimeBasedAlerts(now);
    }
}
