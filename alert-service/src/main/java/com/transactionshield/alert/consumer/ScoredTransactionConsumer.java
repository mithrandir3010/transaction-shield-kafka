package com.transactionshield.alert.consumer;

import com.transactionshield.alert.entity.Alert;
import com.transactionshield.alert.producer.AlertEventProducer;
import com.transactionshield.alert.service.AlertService;
import com.transactionshield.common.event.ScoredTransactionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumes ScoredTransactionEvent from `transactions.scored`.
 *
 * Processing order:
 *   [1] Persist alert to PostgreSQL   (@Retryable: 3 attempts for transient DB errors)
 *   [2] If HIGH|CRITICAL → publish AlertCreatedEvent to `alerts.created`
 *   [3] Acknowledge Kafka offset
 *
 * Failure behaviour:
 *   - DB failure after retries        → AlertPersistenceException propagates
 *   - Kafka publish failure           → exception propagates
 *   - Either failure                  → no ack, DefaultErrorHandler retries
 *   - Exhausted Kafka retries         → DeadLetterPublishingRecoverer → DLQ
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ScoredTransactionConsumer {

    private final AlertService       alertService;
    private final AlertEventProducer alertEventProducer;

    @KafkaListener(
            topics           = "${app.kafka.transactions-scored-topic}",
            groupId          = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload  ScoredTransactionEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC)     String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int    partition,
            @Header(KafkaHeaders.OFFSET)             long   offset,
            Acknowledgment ack) throws Exception {

        log.info("ScoredTransaction received — transactionId={} fraudScore={} riskLevel={} offset={}",
                event.transactionId(), event.fraudScore(), event.riskLevel(), offset);

        // [1] Persist — @Retryable handles transient DataAccessExceptions
        Alert saved = alertService.saveAlert(event);

        // [2] Notify downstream for HIGH / CRITICAL
        if (alertService.shouldNotify(event)) {
            log.info("Risk level {} triggers notification — transactionId={}",
                    event.riskLevel(), event.transactionId());
            alertEventProducer.publish(saved);
        }

        // [3] Commit offset — only reached on full success
        ack.acknowledge();
        log.debug("Offset committed — partition={} offset={}", partition, offset);
    }
}
