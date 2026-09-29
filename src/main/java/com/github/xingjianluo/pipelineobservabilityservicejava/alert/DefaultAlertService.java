package com.github.xingjianluo.pipelineobservabilityservicejava.alert;

import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineConfig;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineStatusSnapshot;
import com.github.xingjianluo.pipelineobservabilityservicejava.store.StateStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 默认告警服务。
 *
 * <p>对每种 {@link AlertType} 维护一个 {@link AlertState}：</p>
 * <ul>
 *   <li>条件成立但 active=false：立即告警，置 active、记录时间、开抑制窗口；</li>
 *   <li>条件成立且 active=true：抑制窗口内只发一次；窗口过后允许再次提醒；</li>
 *   <li>条件不成立：active 复位为 false（问题恢复），之后再出问题可重新触发。</li>
 * </ul>
 */
@Component
public class DefaultAlertService implements AlertService {

    private static final Logger log = LoggerFactory.getLogger(DefaultAlertService.class);

    private final StateStore store;
    private final Map<String, Map<AlertType, AlertState>> states = new ConcurrentHashMap<>();
    private final List<Alert> history = new ArrayList<>();

    public DefaultAlertService(StateStore store) {
        this.store = store;
    }

    @PostConstruct
    public void load() {
        states.clear();
        states.putAll(store.loadAlertStates());
        history.clear();
        history.addAll(store.loadAlerts());
        log.info("恢复告警状态: {} 条流水线, 历史告警 {} 条", states.size(), history.size());
    }

    @Override
    public List<Alert> evaluate(PipelineStatusSnapshot snap, PipelineConfig config, Instant now) {
        return evaluate(snap, config, now, false);
    }

    @Override
    public List<Alert> evaluate(PipelineStatusSnapshot snap, PipelineConfig config,
                                Instant now, boolean timeBasedProbe) {
        Duration window = config != null && config.getAlertSuppressWindow() != null
                ? config.getAlertSuppressWindow() : Duration.ofMinutes(15);
        boolean slaCondition;
        boolean staleCondition;
        if (timeBasedProbe) {
            // 巡检路径：真实时钟推进，延迟完成与运行中超时、新鲜度都要评估。
            slaCondition = snap.slaBreached();
            staleCondition = snap.stale();
        } else {
            // 事件接入路径：事件刚发生（now=receivedAt），"运行中超时"和"长时间无成功"
            // 属于纯时间流逝型条件，只能由巡检用真实时钟判定；这里只报事件直接推出的：
            // 连续失败、延迟完成（终态 endedAt 晚于 SLA 截止）。
            slaCondition = snap.slaBreached() && !snap.runningPastSla();
            staleCondition = false;
        }
        Map<AlertType, Boolean> conditions = Map.of(
                AlertType.CONSECUTIVE_FAILURES, snap.failureThresholdExceeded(),
                AlertType.SLA_BREACHED, slaCondition,
                AlertType.STALE_NO_SUCCESS, staleCondition);

        List<Alert> fired = new ArrayList<>();
        conditions.forEach((type, condition) -> {
            if (!condition) {
                recover(snap.pipelineName(), type, now);
                return;
            }
            Alert alert = maybeFire(snap, type, window, now);
            if (alert != null) {
                fired.add(alert);
            }
        });
        return fired;
    }

    private void recover(String pipeline, AlertType type, Instant now) {
        AlertState st = get(pipeline, type);
        if (st.isActive()) {
            log.info("告警恢复: pipeline={} type={} @ {}", pipeline, type, now);
        }
        st.setActive(false);
        st.setSuppressedUntil(null);
        persist(pipeline, st);
    }

    private Alert maybeFire(PipelineStatusSnapshot snap, AlertType type, Duration window, Instant now) {
        String pipeline = snap.pipelineName();
        AlertState st = get(pipeline, type);

        if (st.isActive() && st.getSuppressedUntil() != null && now.isBefore(st.getSuppressedUntil())) {
            log.info("告警抑制中: pipeline={} type={} 下次可发>={}", pipeline, type, st.getSuppressedUntil());
            return null;
        }

        st.setActive(true);
        st.setLastFiredAt(now);
        st.setSuppressedUntil(now.plus(window));
        st.setFireCount(st.getFireCount() + 1);
        persist(pipeline, st);

        String message = buildMessage(type, snap);
        Alert alert = new Alert(pipeline, currentRunId(snap), type, now, message);
        history.add(alert);
        store.appendAlert(alert);
        log.warn("发出告警: {} @ {}", alert, now);
        return alert;
    }

    private String currentRunId(PipelineStatusSnapshot snap) {
        // runId 级别信息在消息里说明即可，快照不含 runId。
        return null;
    }

    private String buildMessage(AlertType type, PipelineStatusSnapshot snap) {
        return switch (type) {
            case CONSECUTIVE_FAILURES -> "连续失败 " + snap.consecutiveFailures() + " 次";
            case SLA_BREACHED -> "运行超过 SLA 阈值（延迟完成或运行中超时）";
            case STALE_NO_SUCCESS -> "距最近一次成功已超过新鲜度阈值，lastSuccessAt=" + snap.lastSuccessAt();
        };
    }

    private AlertState get(String pipeline, AlertType type) {
        return states.computeIfAbsent(pipeline, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(type, t -> {
                    AlertState s = new AlertState();
                    s.setType(t);
                    return s;
                });
    }

    private void persist(String pipeline, AlertState st) {
        store.saveAlertState(pipeline, st);
    }

    @Override
    public List<AlertType> activeAlerts(String pipelineName) {
        Map<AlertType, AlertState> m = states.get(pipelineName);
        if (m == null) {
            return List.of();
        }
        List<AlertType> r = new ArrayList<>();
        for (AlertType t : AlertType.values()) {
            AlertState s = m.get(t);
            if (s != null && s.isActive()) {
                r.add(t);
            }
        }
        return r;
    }

    @Override
    public List<Alert> history(String pipelineName) {
        return history.stream().filter(a -> a.pipelineName().equals(pipelineName)).toList();
    }

    /** 全部历史（测试用）。 */
    public List<Alert> allHistory() {
        return List.copyOf(history);
    }
}
