package com.github.xingjianluo.pipelineobservabilityservicejava.alert;

/** 告警出口：默认落日志，测试可替换为收集型实现以断言告警条数。 */
public interface AlertSink {
    void send(AlertNotification notification);
}
