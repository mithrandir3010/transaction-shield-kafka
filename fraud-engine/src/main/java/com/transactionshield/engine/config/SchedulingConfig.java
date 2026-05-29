package com.transactionshield.engine.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Activates Spring's @Scheduled (for rule config refresh) and @Async
 * (for non-blocking audit log writes).
 */
@Configuration
@EnableScheduling
@EnableAsync
public class SchedulingConfig {}
