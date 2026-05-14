package com.transactionshield.engine.support;

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

/**
 * Test yardımcı sınıfı — transactions.scored topic'ini dinler ve
 * alınan ScoredTransactionEvent'leri bir BlockingQueue'da biriktirir.
 *
 * Neden BlockingQueue?
 *   Kafka consumer ve test assertion farklı thread'lerde çalışır.
 *   BlockingQueue.poll(timeout) thread-safe bir şekilde bekleme sağlar,
 *   Awaitility'ye gerek kalmadan async sonuçları yakalar.
 *
 * Neden @Component (test source'da)?
 *   @SpringBootTest FraudEngineApplication'ı tarar → com.transactionshield.engine
 *   altındaki tüm bean'leri algılar. Test source'daki bu sınıf aynı paket
 *   altında olduğundan otomatik algılanır.
 *
 * Dikkat: @BeforeEach ile clear() çağrılmazsa önceki test metodunun
 * event'leri kuyruğa karışabilir. AbstractIntegrationTest bunu halleder.
 */
@Component
@Slf4j
public class ScoredEventCollector {

    private final BlockingQueue<ScoredTransactionEvent> queue = new LinkedBlockingQueue<>();

    @KafkaListener(
            topics           = "${app.kafka.transactions-scored-topic}",
            groupId          = "integration-test-consumer",
            containerFactory = "testKafkaListenerContainerFactory"
    )
    public void collect(ScoredTransactionEvent event) {
        log.info("[TEST COLLECTOR] Received — transactionId={} fraudScore={} riskLevel={}",
                event.transactionId(), event.fraudScore(), event.riskLevel());
        queue.add(event);
    }

    /**
     * Belirtilen süre içinde kuyruktaki ilk event'i döner ve kuyruktan kaldırır.
     *
     * @return event, yoksa null (timeout dolduğunda)
     */
    public ScoredTransactionEvent poll(Duration timeout) throws InterruptedException {
        return queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Belirtilen süre içinde N adet event toplar.
     * Velocity testi gibi çok event beklendiğinde kullanılır.
     */
    public List<ScoredTransactionEvent> pollN(int count, Duration perEventTimeout)
            throws InterruptedException {
        List<ScoredTransactionEvent> results = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ScoredTransactionEvent event = poll(perEventTimeout);
            if (event == null) break;   // timeout — beklenen sayıya ulaşılamadı
            results.add(event);
        }
        return results;
    }

    /** Kuyruğu temizler — test metodları arası izolasyon için @BeforeEach'te çağrılır. */
    public void clear() {
        int size = queue.size();
        queue.clear();
        if (size > 0) {
            log.warn("[TEST COLLECTOR] Cleared {} leftover event(s) from previous test", size);
        }
    }
}
