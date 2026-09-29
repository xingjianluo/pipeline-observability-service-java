package com.github.xingjianluo.pipelineobservabilityservicejava.web;

import com.github.xingjianluo.pipelineobservabilityservicejava.alert.Alert;
import com.github.xingjianluo.pipelineobservabilityservicejava.audit.DecisionRecord;
import com.github.xingjianluo.pipelineobservabilityservicejava.service.ObservabilityService;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineConfig;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineStatusSnapshot;
import com.github.xingjianluo.pipelineobservabilityservicejava.web.dto.ConfigRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.util.List;

/** 状态/判定/告警查询与配置入口。 */
@RestController
@RequestMapping("/api")
public class QueryController {

    private final ObservabilityService service;
    private final Clock clock;

    public QueryController(ObservabilityService service, Clock clock) {
        this.service = service;
        this.clock = clock;
    }

    @GetMapping("/pipelines/{name}/status")
    public PipelineStatusSnapshot status(@PathVariable String name) {
        return service.status(name, clock.instant());
    }

    @GetMapping("/pipelines/{name}/decisions")
    public List<DecisionRecord> decisions(@PathVariable String name) {
        return service.decisions(name);
    }

    @GetMapping("/pipelines/{name}/alerts")
    public List<Alert> alerts(@PathVariable String name) {
        return service.alerts(name);
    }

    @PostMapping("/pipelines/config")
    public PipelineConfig configure(@RequestBody ConfigRequest request) {
        PipelineConfig c = new PipelineConfig();
        c.setPipelineName(request.getPipelineName());
        c.setFreshnessThreshold(request.getFreshnessThreshold());
        c.setSla(request.getSla());
        c.setFailureAlertThreshold(request.getFailureAlertThreshold() != null
                ? request.getFailureAlertThreshold() : 0);
        c.setAlertSuppressWindow(request.getAlertSuppressWindow());
        return service.configure(c);
    }
}
