package com.transactionshield.engine.consumer;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Monitors the Dead Letter Queue topic for observability.
 *
 * Responsibilities:
 *  - Logs all DLQ arrivals with exception metadata (preserved in headers)
 *  - Increments the dlq.message.total counter for alerting
 *  - Acts as the entry point for future alert/notification integrations
 *
 * This consumer does NOT attempt reprocessing — DLQ messages require
 * human investigation or a dedicated replay mechanism.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DlqConsumer {

    private final MeterRegistry meterRegistry;

    @KafkaListener(
            topics  = "${app.kafka.transactions-dlq-topic}",
            groupId = "${spring.kafka.consumer.group-id}-dlq"
    )
    public void consume(
            ConsumerRecord<String, Object> record,
            @Header(name = "kafka_dlt-exception-fqcn",     required = false) String exceptionClass,
            @Header(name = "kafka_dlt-exception-message",  required = false) String exceptionMessage,
            @Header(name = "kafka_dlt-original-topic",     required = false) String originalTopic,
            @Header(name = "kafka_dlt-original-offset",    required = false) Long   originalOffset) {

        meterRegistry.counter("dlq.message.total",
                "original_topic", originalTopic != null ? originalTopic : "unknown").increment();

        log.error("""
                [DLQ] Dead letter received
                  key            : {}
                  originalTopic  : {}
                  originalOffset : {}
                  exceptionClass : {}
                  exceptionMsg   : {}
                  payload        : {}
                """,
                record.key(), originalTopic, originalOffset,
                exceptionClass, exceptionMessage, record.value());
    }
}
