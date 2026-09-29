package com.github.xingjianluo.pipelineobservabilityservicejava.web;

import com.github.xingjianluo.pipelineobservabilityservicejava.event.EventIngestResult;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.RejectReason;
import com.github.xingjianluo.pipelineobservabilityservicejava.service.PipelineObservabilityService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 运行事件接入。
 * 接受 → 200；重复（幂等）→ 200 并标注 duplicate=true；
 * 校验拒绝 → 422 并带具体 rejectReason，绝不静默丢弃（另落 rejected 留痕）。
 */
@RestController
@RequestMapping("/api/events")
public class EventController {

    private final PipelineObservabilityService service;

    public EventController(PipelineObservabilityService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<?> ingest(@RequestBody(required = false) String rawJson) {
        EventIngestResult result = service.ingestRaw(rawJson);
        if (!result.isAccepted()) {
            RejectReason reason = result.getRejectReason();
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(Map.of(
                    "accepted", false,
                    "rejectReason", reason.name(),
                    "message", result.getMessage()
            ));
        }
        return ResponseEntity.ok(Map.of(
                "accepted", true,
                "duplicate", result.isDuplicate(),
                "message", result.getMessage()
        ));
    }
}
