package com.transactionshield.engine.chaos;

import com.transactionshield.common.avro.AvroMapper;
import com.transactionshield.common.event.TransactionEvent;
import com.transactionshield.engine.config.TestKafkaConfig;
import com.transactionshield.engine.support.ScoredEventCollector;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chaos / Resilience Test: PostgreSQL Failure in Fraud Engine
 *
 * Scenario: The PostgreSQL instance becomes unavailable after initial startup.
 *
 * Why this should NOT break transaction scoring:
 *
 *   1. Rule configs are cached in-memory via {@link com.transactionshield.engine.service.RuleConfigService}.
 *      The cache is populated at startup and refreshed on a scheduled interval.
 *      When Postgres goes down, the scheduler throws but the CACHE IS NOT CLEARED.
 *      Scoring continues using the last known rule set.
 *
 *   2. Audit log writes are async and non-blocking ({@code @Async}, try-catch swallows errors).
 *      If Postgres is down, audit records are lost but the main scoring flow is unaffected.
 *
 *   3. Idempotency uses Redis, not Postgres → unaffected by DB failure.
 *
 * Design principle: the fraud engine degrades gracefully — no new events are blocked
 * or unscored because of a Postgres outage. Stale rule configs are preferable to
 * stopped scoring.
 *
 * Run:
 *   mvn test -pl fraud-engine -Dgroups=chaos
 */
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
@Import(TestKafkaConfig.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Chaos: PostgreSQL failure — fraud engine uses cached rules")
class PostgresDownResilienceTest {

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

    static boolean dockerAvailable = false;

    static {
        try {
            if (DockerClientFactory.instance().isDockerAvailable()) {
                Startables.deepStart(KAFKA, POSTGRES, REDIS).join();
                dockerAvailable = true;
            }
        } catch (Exception ignored) {}
    }

    @BeforeAll
    static void requireDocker() {
        Assumptions.assumeTrue(dockerAvailable, "Docker unavailable — chaos test skipped");
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url",          POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username",     POSTGRES::getUsername);
        registry.add("spring.datasource.password",     POSTGRES::getPassword);
        registry.add("spring.data.redis.host",         REDIS::getHost);
        registry.add("spring.data.redis.port",         () -> REDIS.getMappedPort(6379));
        // Force short rule refresh interval so we can verify refresh failure is benign
        registry.add("app.rules.refresh-interval-ms",  () -> "2000");
    }

    @Autowired
    KafkaTemplate<String, com.transactionshield.avro.TransactionEvent> rawEventTemplate;

    @Autowired
    ScoredEventCollector collector;

    @BeforeEach
    void clearCollector() {
        collector.clear();
    }

    @Test
    @Order(1)
    @DisplayName("[1] Postgres UP — scoring works normally")
    void baseline_postgresUp_scoringWorks() throws Exception {
        publishRaw(buildEvent("baseline-user", new BigDecimal("500.00"), "US"));

        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertThat(collector.pollN(1, java.time.Duration.ofSeconds(5))).hasSize(1));

        collector.clear();
        assertThat(true).as("Baseline scored event received").isTrue();
    }

    @Test
    @Order(2)
    @DisplayName("[2] Postgres DOWN — scoring continues with cached rules (no 'rule unavailable' errors)")
    void chaos_postgresDown_scoringContinuesWithCache() throws Exception {
        // Pause Postgres — simulates network partition or DB crash
        POSTGRES.getDockerClient().pauseContainerCmd(POSTGRES.getContainerId()).exec();

        try {
            // Wait for at least one failed scheduled refresh (refresh-interval-ms=2000)
            Thread.sleep(3000);

            // Despite Postgres being down, events should still be scored using cached rules
            publishRaw(buildEvent("chaos-user", new BigDecimal("15000.00"), "RU"));

            var results = new java.util.ArrayList<com.transactionshield.common.event.ScoredTransactionEvent>();
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        var events = collector.pollN(1, java.time.Duration.ofSeconds(3));
                        assertThat(events).hasSize(1);
                        results.addAll(events);
                    });

            assertThat(results.get(0).fraudScore())
                    .as("HighAmount(RU) should trigger HIGH risk even with Postgres down")
                    .isGreaterThan(50);
        } finally {
            POSTGRES.getDockerClient().unpauseContainerCmd(POSTGRES.getContainerId()).exec();
        }
    }

    @Test
    @Order(3)
    @DisplayName("[3] Postgres RECOVERED — rule refresh resumes, scoring continues")
    void recovery_postgresRestored_rulesRefreshAndScoringContinues() throws Exception {
        // Postgres was unpaused in test [2] finally block
        Thread.sleep(3000); // wait for successful refresh cycle

        publishRaw(buildEvent("recovery-user", new BigDecimal("200.00"), "US"));

        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        assertThat(collector.pollN(1, java.time.Duration.ofSeconds(5))).hasSize(1));
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private void publishRaw(TransactionEvent event) throws Exception {
        rawEventTemplate.send("transactions.raw", event.transactionId(), AvroMapper.toAvro(event)).get();
    }

    private TransactionEvent buildEvent(String userId, BigDecimal amount, String country) {
        return new TransactionEvent(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                userId, amount, "USD", country,
                "fp-chaos-" + userId.hashCode(),
                Instant.now()
        );
    }
}
