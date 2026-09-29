package com.github.xingjianluo.pipelineobservabilityservicejava.web;

import com.github.xingjianluo.pipelineobservabilityservicejava.dto.RejectedEvent;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.PipelineDerivedStatus;
import com.github.xingjianluo.pipelineobservabilityservicejava.service.PipelineObservabilityService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** 状态查询：全量/单条流水线派生状态，以及被拒事件留痕查询。 */
@RestController
@RequestMapping("/api")
public class PipelineController {

    private final PipelineObservabilityService service;

    public PipelineController(PipelineObservabilityService service) {
        this.service = service;
    }

    @GetMapping("/pipelines")
    public List<PipelineDerivedStatus> all() {
        return service.allStatuses();
    }

    @GetMapping("/pipelines/{name}/status")
    public PipelineDerivedStatus status(@PathVariable("name") String name) {
        return service.statusOf(name);
    }

    @GetMapping("/events/rejected")
    public List<RejectedEvent> rejected() {
        return service.rejectedEvents();
    }
}
