package com.github.xingjianluo.pipelineobservabilityservicejava.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/** 注入系统时钟；测试可用固定/可控时钟替换。 */
@Configuration
public class ClockConfig {

    @Bean
    public Clock systemClock() {
        return Clock.systemUTC();
    }
}
