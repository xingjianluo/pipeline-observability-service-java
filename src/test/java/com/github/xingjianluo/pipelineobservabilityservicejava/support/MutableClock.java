package com.github.xingjianluo.pipelineobservabilityservicejava.support;

import java.util.concurrent.atomic.AtomicLong;

/** 测试用可控时钟：可设定/推进时间，制造迟到、超时、重启后的时间流逝。 */
public class MutableClock implements com.github.xingjianluo.pipelineobservabilityservicejava.support.Clock {

    private final AtomicLong now;

    public MutableClock(long start) {
        this.now = new AtomicLong(start);
    }

    public void set(long millis) {
        now.set(millis);
    }

    public void advance(long deltaMs) {
        now.addAndGet(deltaMs);
    }

    @Override
    public long nowMillis() {
        return now.get();
    }
}
