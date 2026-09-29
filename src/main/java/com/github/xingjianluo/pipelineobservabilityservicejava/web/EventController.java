package com.github.xingjianluo.pipelineobservabilityservicejava.web;

import com.github.xingjianluo.pipelineobservabilityservicejava.audit.DecisionRecord;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RejectedEventException;
import com.github.xingjianluo.pipelineobservabilityservicejava.service.ObservabilityService;
import com.github.xingjianluo.pipelineobservabilityservicejava.web.dto.RejectResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;

/** 事件接入入口。 */
@RestController
@RequestMapping("/api")
public class EventController {

    private final ObservabilityService service;
    private final Clock clock;

    public EventController(ObservabilityService service, Clock clock) {
        this.service = service;
        this.clock = clock;
    }

    @PostMapping("/events")
    public DecisionRecord ingest(@RequestBody String rawPayload) {
        return service.ingest(rawPayload, clock.instant());
    }

    @ExceptionHandler(RejectedEventException.class)
    public ResponseEntity<RejectResponse> reject(RejectedEventException ex) {
        return ResponseEntity.badRequest()
                .body(new RejectResponse(ex.getReason().name(), ex.getMessage()));
    }
}
