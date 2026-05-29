package com.transactionshield.alert;

import com.transactionshield.alert.config.TestAlertKafkaConfig;
import com.transactionshield.alert.support.AlertCreatedEventCollector;
import com.transactionshield.alert.support.DlqMessageCollector;
import com.transactionshield.common.avro.AvroMapper;
import com.transactionshield.common.enums.RiskLevel;
import com.transactionshield.common.event.ScoredTransactionEvent;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * alert-service integration testleri için temel sınıf.
 *
 * Konteyner:
 *   - Kafka (Testcontainers)      → transactions.scored, alerts.created, transactions.dlq
 *   - PostgreSQL (Testcontainers) → Flyway V1__ migration ile schema oluşturulur
 *
 * Paralel başlatma: Startables.deepStart() ile her iki konteyner eş zamanlı başlar.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
@Import(TestAlertKafkaConfig.class)
public abstract class AbstractAlertIntegrationTest {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"));

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("transactionshield")
                    .withUsername("tsuser")
                    .withPassword("tspassword");

    static boolean dockerAvailable = false;

    static {
        try {
            if (DockerClientFactory.instance().isDockerAvailable()) {
                Startables.deepStart(KAFKA, POSTGRES).join();
                dockerAvailable = true;
            }
        } catch (Exception ignored) {
            // Docker unavailable — tests skipped via @BeforeAll
        }
    }

    @BeforeAll
    static void requireDocker() {
        Assumptions.assumeTrue(dockerAvailable,
                "Docker not available — integration tests skipped");
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url",          POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username",     POSTGRES::getUsername);
        registry.add("spring.datasource.password",     POSTGRES::getPassword);
    }

    @Autowired protected KafkaTemplate<String, com.transactionshield.avro.ScoredTransactionEvent> scoredEventTemplate;
    @Autowired protected AlertCreatedEventCollector alertCreatedCollector;
    @Autowired protected DlqMessageCollector dlqCollector;

    @BeforeEach
    void clearCollectors() {
        alertCreatedCollector.clear();
        dlqCollector.clear();
    }

    protected void publishScored(ScoredTransactionEvent event) throws Exception {
        scoredEventTemplate.send("transactions.scored", event.transactionId(), AvroMapper.toAvro(event)).get();
    }

    // ── Builder Helpers ───────────────────────────────────────────────

    protected ScoredTransactionEvent buildScoredEvent(String txId, RiskLevel level, int score,
                                                       List<String> rules) {
        return new ScoredTransactionEvent(
                txId,
                "idem-" + UUID.randomUUID(),
                "user-int-001",
                BigDecimal.valueOf(score > 50 ? 15000 : 500),
                "USD",
                level == RiskLevel.CRITICAL ? "RU" : "US",
                "fp-int",
                Instant.now(),
                score,
                score,
                level,
                rules,
                Instant.now()
        );
    }
}
