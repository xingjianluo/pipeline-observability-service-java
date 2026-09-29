package com.github.xingjianluo.pipelineobservabilityservicejava.support;

/** 可注入时钟，测试用固定时钟制造迟到/超时/重启后的时间流逝。 */
public interface Clock {
    long nowMillis();
}
