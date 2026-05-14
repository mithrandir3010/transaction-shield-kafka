package com.transactionshield.engine;

import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.config.TestKafkaConfig;
import com.transactionshield.engine.support.ScoredEventCollector;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Tüm integration testleri için temel sınıf.
 *
 * Konteyner stratejisi — Singleton + Paralel başlatma:
 *   static final alanlar JVM genelinde tek kez başlatılır → test sınıfları
 *   arasında paylaşılır. Üç konteynerin paralel başlatılması için
 *   Startables.deepStart() kullanılır; toplam bekleme süresi
 *   max(K, P, R) olur, K+P+R yerine.
 *
 * Neden @Testcontainers yerine static initializer?
 *   @Testcontainers + static @Container: test sınıfı başına konteyner
 *   başlatır (AbstractIntegrationTest çalıştırılmaz ama alt sınıflar her
 *   biri ayrı birer test sınıfıdır).
 *   static initializer: gerçek singleton — JVM lifetime boyunca bir kez.
 *   Çok sayıda test sınıfı olduğunda bu, Docker overhead'i önemli ölçüde azaltır.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
@ActiveProfiles("test")
@Import(TestKafkaConfig.class)
public abstract class AbstractIntegrationTest {

    // ── Containers ───────────────────────────────────────────────────

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"));

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("transactionshield")
                    .withUsername("tsuser")
                    .withPassword("tspassword");

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
                    .withExposedPorts(6379)
                    .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    // Paralel başlatma — üç konteyner eş zamanlı ayağa kalkar
    static {
        Startables.deepStart(KAFKA, POSTGRES, REDIS).join();
    }

    // ── Spring Context Wiring ────────────────────────────────────────

    /**
     * @DynamicPropertySource: Testcontainers'ın dinamik portlarını Spring'e bildirir.
     * Bu annotation olmadan Spring, localhost:9092 gibi sabit portlara bağlanmaya çalışır.
     */
    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url",          POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username",     POSTGRES::getUsername);
        registry.add("spring.datasource.password",     POSTGRES::getPassword);
        registry.add("spring.data.redis.host",         REDIS::getHost);
        registry.add("spring.data.redis.port",         () -> REDIS.getMappedPort(6379));
    }

    // ── Shared Test Beans ────────────────────────────────────────────

    @Autowired
    protected KafkaTemplate<String, TransactionEvent> rawEventTemplate;

    @Autowired
    protected ScoredEventCollector collector;

    /**
     * Her test metodundan önce collector kuyruğunu temizle.
     * Önceki test metodunun ürettiği scored event'ler bir sonraki testi kirletmesin.
     */
    @BeforeEach
    void clearCollector() {
        collector.clear();
    }

    // ── Builder Helpers ───────────────────────────────────────────────

    protected TransactionEvent buildEvent(String userId, BigDecimal amount, String country) {
        return new TransactionEvent(
                UUID.randomUUID().toString(),   // transactionId
                UUID.randomUUID().toString(),   // idempotencyKey
                userId,
                amount,
                "USD",
                country,
                "device-fp-" + userId.hashCode(),
                Instant.now()
        );
    }

    /**
     * Gece 02:30 UTC zaman damgalı event — NightTransactionRule (00:00–05:00) tetiklenir.
     * TransactionEvent.timestamp alanı kontrolümüzde olduğu için geçmiş bir tarihi
     * kullanmak yeterlidir; rule sadece saate bakar.
     */
    protected TransactionEvent buildNightEvent(String userId, BigDecimal amount, String country) {
        Instant nightTs = LocalDate.now(ZoneOffset.UTC)
                .atTime(2, 30)
                .toInstant(ZoneOffset.UTC);
        return new TransactionEvent(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                userId,
                amount,
                "USD",
                country,
                "device-fp-" + userId.hashCode(),
                nightTs
        );
    }
}
