package com.github.xingjianluo.pipelineobservabilityservicejava.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/** 兜底：Spring 层无法读取/解析请求体时也按 MALFORMED_JSON 返回 422。 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<?> unreadable(HttpMessageNotReadableException e) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(Map.of(
                "accepted", false,
                "rejectReason", "MALFORMED_JSON",
                "message", "request body is not readable"
        ));
    }
}
