package com.transactionshield.engine.support;

import com.transactionshield.common.avro.AvroMapper;
import com.transactionshield.common.event.ScoredTransactionEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class ScoredEventCollector {

    private final BlockingQueue<ScoredTransactionEvent> queue = new LinkedBlockingQueue<>();

    @KafkaListener(
            topics           = "${app.kafka.transactions-scored-topic}",
            groupId          = "integration-test-consumer",
            containerFactory = "testKafkaListenerContainerFactory"
    )
    public void collect(com.transactionshield.avro.ScoredTransactionEvent avroEvent) {
        ScoredTransactionEvent event = AvroMapper.fromAvro(avroEvent);
        log.info("[TEST COLLECTOR] Received — transactionId={} fraudScore={} riskLevel={}",
                event.transactionId(), event.fraudScore(), event.riskLevel());
        queue.add(event);
    }

    public ScoredTransactionEvent poll(Duration timeout) throws InterruptedException {
        return queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public List<ScoredTransactionEvent> pollN(int count, Duration perEventTimeout)
            throws InterruptedException {
        List<ScoredTransactionEvent> results = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ScoredTransactionEvent event = poll(perEventTimeout);
            if (event == null) break;
            results.add(event);
        }
        return results;
    }

    public void clear() {
        int size = queue.size();
        queue.clear();
        if (size > 0) {
            log.warn("[TEST COLLECTOR] Cleared {} leftover event(s) from previous test", size);
        }
    }
}
