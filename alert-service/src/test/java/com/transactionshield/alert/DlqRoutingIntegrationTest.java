package com.transactionshield.alert;

import com.transactionshield.alert.config.TestAlertKafkaConfig;
import com.transactionshield.alert.exception.AlertPersistenceException;
import com.transactionshield.alert.service.AlertService;
import com.transactionshield.alert.support.DlqMessageCollector;
import com.transactionshield.common.enums.RiskLevel;
import com.transactionshield.common.event.ScoredTransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.backoff.FixedBackOff;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

/**
 * DLQ Routing Integration Testi
 *
 * Senaryo:
 *   AlertService.saveAlert() kalıcı olarak AlertPersistenceException fırlatır
 *   → Kafka DefaultErrorHandler retry'larını tüketir (fast backoff ile hızlandırılmış)
 *   → DeadLetterPublishingRecoverer → transactions.dlq
 *
 * @MockBean AlertService: Servis seviyesinde başarısızlık simüle eder.
 *   (AlertRepository değil AlertService mock'lanır → Spring Retry katmanı devre dışı)
 *
 * FastErrorHandlerConfig (iç @TestConfiguration):
 *   Kafka-level retry backoff'unu sıfıra indirir → test süresi ~0ms × 2 retry.
 *   @Primary + allow-bean-definition-overriding=true ile üretim errorHandler'ı gölgeler.
 *
 * Neden sadece bu test sınıfında geçerli?
 *   @TestConfiguration iç sınıfı sadece bu @SpringBootTest context'inde aktif olur.
 *   AbstractAlertIntegrationTest context'i etkilenmez — farklı Spring context cache key.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties     = "spring.main.allow-bean-definition-overriding=true"
)
@Testcontainers
@ActiveProfiles("test")
@Import(TestAlertKafkaConfig.class)
@DisplayName("DLQ Routing — Kafka retry exhaustion sonrası DLQ yönlendirme")
class DlqRoutingIntegrationTest {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"));

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("transactionshield")
                    .withUsername("tsuser")
                    .withPassword("tspassword")
                    .withInitScript("db/init.sql");

    static {
        Startables.deepStart(KAFKA, POSTGRES).join();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url",          POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username",     POSTGRES::getUsername);
        registry.add("spring.datasource.password",     POSTGRES::getPassword);
    }

    /**
     * Üretim alertErrorHandler'ının yerini alan hızlı versiyon.
     * FixedBackOff(0, 2): 0ms bekleme, 2 retry → 3 toplam Kafka deneme.
     * @Primary: aynı tip birden fazla bean olduğunda bu bean tercih edilir.
     *
     * spring.main.allow-bean-definition-overriding=true olmadan
     * BeanDefinitionOverrideException fırlatır.
     */
    @TestConfiguration
    static class FastErrorHandlerConfig {

        @Bean
        @Primary
        DefaultErrorHandler alertErrorHandler(
                @org.springframework.beans.factory.annotation.Qualifier("dlqKafkaTemplate")
                KafkaTemplate<String, Object> dlqKafkaTemplate) {
            DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                    dlqKafkaTemplate,
                    (record, ex) -> new TopicPartition("transactions.dlq", -1)
            );
            return new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 2L));
        }
    }

    // @MockBean AlertService: save her zaman AlertPersistenceException fırlatır.
    // Spring Retry katmanını (AlertService @Retryable) devre dışı bırakır.
    // Sadece Kafka-level retry + DLQ davranışını test eder.
    @MockBean AlertService alertService;

    @Autowired KafkaTemplate<String, ScoredTransactionEvent> scoredEventTemplate;
    @Autowired DlqMessageCollector dlqCollector;

    @BeforeEach
    void setUp() {
        dlqCollector.clear();
        given(alertService.saveAlert(any()))
                .willThrow(new AlertPersistenceException("Persistent DB failure — DLQ test",
                        new RuntimeException("simulated")));
    }

    @Test
    @DisplayName("Kalıcı AlertService hatası → Kafka retry'ları tüketilir → mesaj DLQ'ya yönlenir")
    void whenPersistentAlertServiceFailure_thenMessageRoutedToDlq() throws Exception {
        String txId = "dlq-test-" + UUID.randomUUID();
        scoredEventTemplate.send("transactions.scored", txId, simpleEvent(txId)).get();

        // FixedBackOff(0, 2): 3 Kafka deneme, 0ms bekleme → hızlı DLQ yönlendirme
        ConsumerRecord<String, String> dlqRecord = dlqCollector.poll(Duration.ofSeconds(30));

        assertThat(dlqRecord)
                .as("Persistent hata sonrası mesaj DLQ'ya yönlenmeli")
                .isNotNull();
        assertThat(dlqRecord.topic()).isEqualTo("transactions.dlq");
        assertThat(dlqRecord.key()).isEqualTo(txId);
    }

    @Test
    @DisplayName("DLQ mesajı — DeadLetterPublishingRecoverer header'larını ekler")
    void whenMessageInDlq_exceptionHeadersPresent() throws Exception {
        String txId = "dlq-hdr-" + UUID.randomUUID();
        scoredEventTemplate.send("transactions.scored", txId, simpleEvent(txId)).get();

        ConsumerRecord<String, String> dlqRecord = dlqCollector.poll(Duration.ofSeconds(30));
        assertThat(dlqRecord).isNotNull();

        // Spring Kafka DeadLetterPublishingRecoverer standart header'ları ekler:
        // kafka_dlt-original-topic, kafka_dlt-exception-message, vb.
        assertThat(dlqRecord.headers().toArray().length)
                .as("DLQ mesajı exception ve kaynak topic header'larını içermeli")
                .isGreaterThan(0);
    }

    @Test
    @DisplayName("Birden fazla başarısız mesaj → her biri DLQ'ya yönlenir")
    void whenMultipleMessagesFail_allRoutedToDlq() throws Exception {
        String txId1 = "dlq-m1-" + UUID.randomUUID();
        String txId2 = "dlq-m2-" + UUID.randomUUID();

        scoredEventTemplate.send("transactions.scored", txId1, simpleEvent(txId1)).get();
        scoredEventTemplate.send("transactions.scored", txId2, simpleEvent(txId2)).get();

        ConsumerRecord<String, String> dlq1 = dlqCollector.poll(Duration.ofSeconds(30));
        ConsumerRecord<String, String> dlq2 = dlqCollector.poll(Duration.ofSeconds(30));

        assertThat(dlq1).as("İlk mesaj DLQ'ya yönlenmeli").isNotNull();
        assertThat(dlq2).as("İkinci mesaj DLQ'ya yönlenmeli").isNotNull();
    }

    private ScoredTransactionEvent simpleEvent(String txId) {
        return new ScoredTransactionEvent(
                txId, "idem-" + txId, "user-dlq",
                BigDecimal.valueOf(100), "USD", "US", null,
                Instant.now(), 0, 0, RiskLevel.LOW,
                List.of(), Instant.now()
        );
    }
}
