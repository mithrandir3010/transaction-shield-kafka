package com.transactionshield.engine.consumer;

import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.service.FraudEngineService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionEventConsumer {

    private final FraudEngineService fraudEngineService;

    /**
     * Consumes from transactions.raw with MANUAL_IMMEDIATE ack mode.
     *
     * Ack flow:
     *  - SUCCESS → ack() commits offset; message is not redelivered
     *  - EXCEPTION → no ack; DefaultErrorHandler retries (up to 3x)
     *                → after max retries: DeadLetterPublishingRecoverer
     *                   publishes to transactions.dlq and acks the original
     *
     * The container factory is defined in KafkaConsumerConfig.
     */
    @KafkaListener(
            topics       = "${app.kafka.transactions-raw-topic}",
            groupId      = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload  TransactionEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC)     String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int    partition,
            @Header(KafkaHeaders.OFFSET)             long   offset,
            Acknowledgment ack) throws Exception {

        log.info("Received — transactionId={} topic={} partition={} offset={}",
                event.transactionId(), topic, partition, offset);

        fraudEngineService.process(event);

        // Commit offset only after successful processing
        ack.acknowledge();

        log.debug("Offset committed — partition={} offset={}", partition, offset);
    }
}
