package com.transactionshield.engine.consumer;

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
 *  - Acts as the entry point for future alert/notification integrations
 *
 * This consumer does NOT attempt reprocessing — DLQ messages require
 * human investigation or a dedicated replay mechanism.
 *
 * Spring Kafka's DeadLetterPublishingRecoverer automatically adds these headers:
 *   kafka_dlt-exception-fqcn     → exception class name
 *   kafka_dlt-exception-message  → exception message
 *   kafka_dlt-original-topic     → source topic
 *   kafka_dlt-original-partition → source partition
 *   kafka_dlt-original-offset    → source offset
 */
@Component
@Slf4j
public class DlqConsumer {

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
