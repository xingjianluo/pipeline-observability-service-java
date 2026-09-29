package com.github.xingjianluo.pipelineobservabilityservicejava.support;

import org.springframework.stereotype.Component;

/** 生产时钟：系统当前时间。 */
@Component
public class SystemClock implements Clock {
    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
