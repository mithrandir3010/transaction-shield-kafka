package com.transactionshield.producer.support;

import com.transactionshield.common.avro.AvroMapper;
import com.transactionshield.common.event.TransactionEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class RawEventCollector {

    private final BlockingQueue<TransactionEvent> queue = new LinkedBlockingQueue<>();

    @KafkaListener(
            topics           = "transactions.raw",
            groupId          = "producer-test-consumer",
            containerFactory = "testRawConsumerFactory"
    )
    public void collect(com.transactionshield.avro.TransactionEvent avroEvent) {
        TransactionEvent event = AvroMapper.fromAvro(avroEvent);
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
