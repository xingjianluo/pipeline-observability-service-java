package com.github.xingjianluo.pipelineobservabilityservicejava.alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** 默认告警出口：把告警以结构化方式写入应用日志。 */
@Component
public class LoggingAlertSink implements AlertSink {

    private static final Logger log = LoggerFactory.getLogger("ALERT");

    @Override
    public void send(AlertNotification n) {
        log.warn("[ALERT] type={} pipeline={} runId={} firedAt={} message={}",
                n.type(), n.pipelineName(), n.runId(), n.firedAt(), n.message());
    }
}
