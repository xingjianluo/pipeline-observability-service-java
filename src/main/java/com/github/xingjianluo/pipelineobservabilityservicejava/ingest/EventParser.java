package com.github.xingjianluo.pipelineobservabilityservicejava.ingest;

import com.github.xingjianluo.pipelineobservabilityservicejava.event.RejectedEventException;
import com.github.xingjianluo.pipelineobservabilityservicejava.event.RunEvent;

import java.time.Instant;

/** 把外部 JSON 请求体解析并严格校验成 RunEvent。校验失败按原因拒绝。 */
public interface EventParser {

    /**
     * @param rawPayload 原始请求体（用于审计）
     * @param now 接入时刻（注入，便于测试）
     */
    RunEvent parse(String rawPayload, Instant now) throws RejectedEventException;
}
