package com.transactionshield.producer.support;

import com.transactionshield.common.event.TransactionEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * transactions.raw topic'ini dinleyen test consumer.
 * TransactionProducerService'in Kafka'ya doğru event publish ettiğini doğrular.
 */
@Component
@Slf4j
public class RawEventCollector {

    private final BlockingQueue<TransactionEvent> queue = new LinkedBlockingQueue<>();

    @KafkaListener(
            topics           = "transactions.raw",
            groupId          = "producer-test-consumer",
            containerFactory = "testRawConsumerFactory"
    )
    public void collect(TransactionEvent event) {
        log.info("[TEST COLLECTOR] Raw event received — transactionId={}", event.transactionId());
        queue.add(event);
    }

    public TransactionEvent poll(Duration timeout) throws InterruptedException {
        return queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void clear() {
        queue.clear();
    }
}
