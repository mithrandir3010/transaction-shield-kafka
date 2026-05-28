package com.transactionshield.producer.service;

import com.transactionshield.common.avro.AvroMapper;
import com.transactionshield.common.dto.TransactionRequest;
import com.transactionshield.common.dto.TransactionResponse;
import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.producer.exception.DuplicateTransactionException;
import com.transactionshield.producer.exception.KafkaPublishException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionProducerService {

    private final IdempotencyService idempotencyService;
    private final KafkaTemplate<String, com.transactionshield.avro.TransactionEvent> kafkaTemplate;

    @Value("${app.kafka.transactions-raw-topic}")
    private String transactionsRawTopic;

    @Value("${app.kafka.publish-timeout-seconds:5}")
    private long publishTimeoutSeconds;

    public TransactionResponse submit(TransactionRequest request) {
        String idempotencyKey = request.idempotencyKey();

        if (!idempotencyService.tryAcquire(idempotencyKey)) {
            throw new DuplicateTransactionException(idempotencyKey);
        }

        TransactionEvent event = new TransactionEvent(
                UUID.randomUUID().toString(),
                idempotencyKey,
                request.userId(),
                request.amount(),
                request.currency(),
                request.country(),
                request.deviceFingerprint(),
                Instant.now()
        );

        try {
            kafkaTemplate.send(transactionsRawTopic, event.transactionId(), AvroMapper.toAvro(event))
                    .get(publishTimeoutSeconds, TimeUnit.SECONDS);

            log.info("Transaction published — transactionId={} idempotencyKey={} topic={}",
                    event.transactionId(), idempotencyKey, transactionsRawTopic);

        } catch (Exception ex) {
            idempotencyService.release(idempotencyKey);
            throw new KafkaPublishException(
                    "Failed to publish transaction " + event.transactionId() + " to Kafka", ex);
        }

        return new TransactionResponse(
                event.transactionId(),
                idempotencyKey,
                "ACCEPTED",
                event.timestamp()
        );
    }
}
