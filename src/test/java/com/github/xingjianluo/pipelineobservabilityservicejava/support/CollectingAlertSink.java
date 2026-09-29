package com.github.xingjianluo.pipelineobservabilityservicejava.support;

import com.github.xingjianluo.pipelineobservabilityservicejava.alert.AlertNotification;
import com.github.xingjianluo.pipelineobservabilityservicejava.alert.AlertSink;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** 收集型告警出口：测试断言告警条数与类别。线程安全。 */
public class CollectingAlertSink implements AlertSink {

    private final List<AlertNotification> notifications = new CopyOnWriteArrayList<>();

    @Override
    public void send(AlertNotification notification) {
        notifications.add(notification);
    }

    public List<AlertNotification> all() {
        return List.copyOf(notifications);
    }

    public long countOf(String type) {
        return notifications.stream().filter(n -> n.type().equals(type)).count();
    }

    public void clear() {
        notifications.clear();
    }
}
