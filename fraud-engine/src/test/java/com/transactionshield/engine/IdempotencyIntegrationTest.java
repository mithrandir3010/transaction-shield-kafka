package com.transactionshield.engine;

import com.transactionshield.common.event.ScoredTransactionEvent;
import com.transactionshield.common.event.TransactionEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ScoringIdempotencyService Integration Testi
 *
 * Gerçek hayat senaryosu:
 *   Kafka at-least-once garantisi → broker restart, network partition
 *   veya consumer commit başarısızlığı durumunda aynı event birden fazla
 *   iletilir. ScoringIdempotencyService bunu Redis SET NX ile engeller.
 *
 * Test yaklaşımı:
 *   Aynı TransactionEvent (aynı transactionId) iki kez gönderilir.
 *   İkinci işlem, FraudEngineService.process() içinde:
 *     ScoringIdempotencyService.tryMarkAsProcessing(transactionId) → false
 *   → işlem atlanır, transactions.scored'a ikinci event yazılmaz.
 *
 * Negatif assertion:
 *   collector.poll(kısa_timeout) → null döndürmeli.
 *   Yani sistem bir şeyin OLMADIĞINI doğrular — Kafka'da gerçek negative
 *   assertion için kısa timeout kullanmak standart yaklaşımdır.
 */
@DisplayName("Scoring Idempotency — Duplicate Event Rejection Tests")
class IdempotencyIntegrationTest extends AbstractIntegrationTest {

    private static final Duration EXPECTED_TIMEOUT  = Duration.ofSeconds(15);
    private static final Duration NEGATIVE_TIMEOUT  = Duration.ofSeconds(4);

    @Test
    @DisplayName("Aynı transactionId iki kez Kafka'ya yazılırsa sadece bir scored event üretilmeli")
    void whenSameTransactionSentTwice_thenOnlyOneEventPublishedToScored() throws Exception {
        // Given — aynı transactionId'ye sahip event (Kafka at-least-once senaryosu)
        TransactionEvent event = buildEvent("u-idem-001", BigDecimal.valueOf(750), "US");

        // When — aynı event iki kez gönderilir
        rawEventTemplate.send("transactions.raw", event.transactionId(), event).get();
        rawEventTemplate.send("transactions.raw", event.transactionId(), event).get();

        // Then — birinci event işlenir ve scored'a yazılır
        ScoredTransactionEvent firstScored = collector.poll(EXPECTED_TIMEOUT);

        assertThat(firstScored)
                .as("İlk event fraud-engine tarafından işlenmeli")
                .isNotNull();
        assertThat(firstScored.transactionId()).isEqualTo(event.transactionId());

        // İkinci event Redis SET NX tarafından bloke edilmeli — kuyrukta ikinci event OLMAMALI
        ScoredTransactionEvent secondScored = collector.poll(NEGATIVE_TIMEOUT);

        assertThat(secondScored)
                .as("Duplicate transactionId için ikinci scored event üretilmemeli (idempotency)")
                .isNull();
    }

    @Test
    @DisplayName("Farklı transactionId'ler bağımsız işlenmeli — idempotency yanlış bloke etmemeli")
    void whenDifferentTransactionsFromSameUser_thenBothProcessed() throws Exception {
        // Given — aynı kullanıcıdan iki FARKLI işlem
        String userId = "u-idem-multi-001";
        TransactionEvent event1 = buildEvent(userId, BigDecimal.valueOf(300), "US");
        TransactionEvent event2 = buildEvent(userId, BigDecimal.valueOf(400), "US");

        // When
        rawEventTemplate.send("transactions.raw", event1.transactionId(), event1).get();
        rawEventTemplate.send("transactions.raw", event2.transactionId(), event2).get();

        // Then — her iki event de bağımsız olarak işlenmeli
        ScoredTransactionEvent scored1 = collector.poll(EXPECTED_TIMEOUT);
        ScoredTransactionEvent scored2 = collector.poll(EXPECTED_TIMEOUT);

        assertThat(scored1).as("İlk event scored olmalı").isNotNull();
        assertThat(scored2).as("İkinci event scored olmalı (farklı transactionId)").isNotNull();

        // İki scored event farklı transactionId'lere sahip olmalı
        assertThat(scored1.transactionId()).isNotEqualTo(scored2.transactionId());
        assertThat(java.util.Set.of(scored1.transactionId(), scored2.transactionId()))
                .containsExactlyInAnyOrder(event1.transactionId(), event2.transactionId());
    }

    @Test
    @DisplayName("Üçlü duplicate — sadece bir scored event üretilmeli")
    void whenSameTransactionSentThreeTimes_thenOnlyOneEventProcessed() throws Exception {
        // Given
        TransactionEvent event = buildEvent("u-triple-dup", BigDecimal.valueOf(999), "DE");

        // When — üç kez gönder (worst-case at-least-once)
        rawEventTemplate.send("transactions.raw", event.transactionId(), event).get();
        rawEventTemplate.send("transactions.raw", event.transactionId(), event).get();
        rawEventTemplate.send("transactions.raw", event.transactionId(), event).get();

        // Then — sadece bir scored event
        ScoredTransactionEvent first  = collector.poll(EXPECTED_TIMEOUT);
        ScoredTransactionEvent second = collector.poll(NEGATIVE_TIMEOUT);
        ScoredTransactionEvent third  = collector.poll(NEGATIVE_TIMEOUT);

        assertThat(first).as("İlk işlem scored olmalı").isNotNull();
        assertThat(second).as("2. duplicate reject edilmeli").isNull();
        assertThat(third).as("3. duplicate reject edilmeli").isNull();
    }
}
