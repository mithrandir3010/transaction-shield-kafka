package com.transactionshield.alert.support;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * transactions.dlq topic'ini dinleyen test consumer.
 * Retry exhausted sonrası DLQ'ya yönlendirilen mesajları yakalar.
 * Raw ConsumerRecord kullanılır — DLQ payload'ı başka servisten gelen binary olabilir.
 */
@Component
@Slf4j
public class DlqMessageCollector {

    private final BlockingQueue<ConsumerRecord<String, String>> queue = new LinkedBlockingQueue<>();

    @KafkaListener(
            topics           = "transactions.dlq",
            groupId          = "dlq-test-consumer",
            containerFactory = "testDlqConsumerFactory"
    )
    public void collect(ConsumerRecord<String, String> record) {
        log.info("[TEST COLLECTOR] DLQ message — key={} topic={} partition={} offset={}",
                record.key(), record.topic(), record.partition(), record.offset());
        queue.add(record);
    }

    public ConsumerRecord<String, String> poll(Duration timeout) throws InterruptedException {
        return queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void clear() {
        queue.clear();
    }
}
