package com.transactionshield.alert.support;

import com.transactionshield.common.event.AlertCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * alerts.created topic'ini dinleyen test consumer.
 * AlertEventProducer'ın HIGH/CRITICAL risk için doğru event publish ettiğini doğrular.
 */
@Component
@Slf4j
public class AlertCreatedEventCollector {

    private final BlockingQueue<AlertCreatedEvent> queue = new LinkedBlockingQueue<>();

    @KafkaListener(
            topics           = "alerts.created",
            groupId          = "alert-created-test-consumer",
            containerFactory = "testAlertCreatedConsumerFactory"
    )
    public void collect(AlertCreatedEvent event) {
        log.info("[TEST COLLECTOR] AlertCreatedEvent — transactionId={} riskLevel={}",
                event.transactionId(), event.riskLevel());
        queue.add(event);
    }

    public AlertCreatedEvent poll(Duration timeout) throws InterruptedException {
        return queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void clear() {
        queue.clear();
    }
}
