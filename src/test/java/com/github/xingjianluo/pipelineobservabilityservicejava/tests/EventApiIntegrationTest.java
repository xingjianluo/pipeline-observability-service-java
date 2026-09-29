package com.github.xingjianluo.pipelineobservabilityservicejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 通过 HTTP 端到端验证：正常事件 200、坏事件 400 且带原因、状态/告警可查。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "observability.storage-dir=${java.io.tmpdir}/pipeline-obs-it-${random.uuid}",
        "observability.default-failure-alert-threshold=1",
        "observability.default-freshness-threshold=3600s",
        "observability.default-sla=3600s",
        "observability.default-suppress-window=60s",
        "observability.monitor-enabled=false"
})
class EventApiIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private HttpEntity<String> json(String body) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<>(body, h);
    }

    @Test
    void ingestRejectQueryEndToEnd() {
        String now = java.time.Instant.now().toString();
        String p = "pipe-http";

        String started = "{\"pipelineName\":\"" + p + "\",\"runId\":\"h1\",\"type\":\"STARTED\",\"eventTime\":\""
                + java.time.Instant.parse(now).minusSeconds(10) + "\"}";
        ResponseEntity<String> r1 = rest.postForEntity(url("/api/events"), json(started), String.class);
        assertEquals(200, r1.getStatusCode().value(), r1.getBody());

        String failed = "{\"pipelineName\":\"" + p + "\",\"runId\":\"h1\",\"type\":\"FAILED\",\"eventTime\":\""
                + java.time.Instant.parse(now).minusSeconds(5) + "\",\"detail\":\"500\"}";
        ResponseEntity<String> r2 = rest.postForEntity(url("/api/events"), json(failed), String.class);
        assertEquals(200, r2.getStatusCode().value(), r2.getBody());

        // 坏事件：400 + 结构化原因。
        ResponseEntity<String> bad = rest.postForEntity(url("/api/events"),
                json("{\"pipelineName\":\"" + p + "\"}"), String.class);
        assertEquals(400, bad.getStatusCode().value());
        assertNotNull(bad.getBody());
        assertTrue(bad.getBody().contains("MISSING_RUN_ID"), bad.getBody());

        // 状态查询。
        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> status = rest.getForObject(
                url("/api/pipelines/" + p + "/status"), java.util.Map.class);
        assertEquals(1, ((Number) status.get("consecutiveFailures")).intValue());

        // 告警查询：阈值=1，应有一条连续失败告警。
        java.util.List<?> alerts = rest.getForObject(
                url("/api/pipelines/" + p + "/alerts"), java.util.List.class);
        assertEquals(1, alerts.size());

        // 判定记录可查。
        java.util.List<?> decisions = rest.getForObject(
                url("/api/pipelines/" + p + "/decisions"), java.util.List.class);
        assertTrue(decisions.size() >= 2);
    }
}
