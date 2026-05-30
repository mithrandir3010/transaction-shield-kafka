package com.transactionshield.producer.chaos;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.transactionshield.producer.config.TestProducerKafkaConfig;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chaos Test: Kafka Broker Failure
 *
 * Scenario: The Kafka broker becomes unavailable during operation.
 *
 * Expected behaviour:
 *   - Requests in-flight during outage → 503 Service Unavailable (after publish timeout)
 *   - Idempotency key is released on Kafka failure → client can safely retry
 *   - No data corruption, no silent data loss
 *
 * Run:
 *   mvn test -pl transaction-producer -Dgroups=chaos
 *   mvn test -pl transaction-producer -Dtest=KafkaDownChaosTest
 */
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
@Import(TestProducerKafkaConfig.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Chaos: Kafka broker failure")
class KafkaDownChaosTest {

    // Own containers — isolated from shared AbstractProducerIntegrationTest containers
    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"));

    @Container
    @SuppressWarnings("resource")
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
                    .withExposedPorts(6379)
                    .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));

    static boolean dockerAvailable = false;
    static String  testJwt;

    static {
        try {
            if (DockerClientFactory.instance().isDockerAvailable()) {
                Startables.deepStart(KAFKA, REDIS).join();
                dockerAvailable = true;
            }
        } catch (Exception ignored) {}
    }

    @BeforeAll
    static void setup() throws Exception {
        Assumptions.assumeTrue(dockerAvailable, "Docker unavailable — chaos test skipped");
        testJwt = generateTestJwt();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host",         REDIS::getHost);
        registry.add("spring.data.redis.port",         () -> REDIS.getMappedPort(6379));
        registry.add("app.kafka.publish-timeout-seconds", () -> "3");  // short timeout for tests
    }

    @Autowired
    TestRestTemplate rest;

    @Test
    @Order(1)
    @DisplayName("[1] Kafka UP → POST /transactions returns 202 Accepted")
    void baseline_kafkaUp_returns202() {
        ResponseEntity<Map> response = rest.exchange(
                "/api/v1/transactions", HttpMethod.POST,
                transactionRequest("chaos-kafka-baseline"), Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).containsKey("transactionId");
    }

    @Test
    @Order(2)
    @DisplayName("[2] Kafka DOWN → POST /transactions returns 503 Service Unavailable")
    void chaos_kafkaDown_returns503() {
        // Pause the Kafka container (simulates network partition / broker crash)
        KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();

        try {
            ResponseEntity<Map> response = rest.exchange(
                    "/api/v1/transactions", HttpMethod.POST,
                    transactionRequest("chaos-kafka-down"), Map.class);

            assertThat(response.getStatusCode())
                    .as("Expected 503 when Kafka is unavailable")
                    .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        } finally {
            // Restore: unpause for potential other tests
            KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }
    }

    @Test
    @Order(3)
    @DisplayName("[3] Kafka RECOVERED → POST /transactions returns 202 (system self-heals)")
    void recovery_kafkaRestored_returns202() {
        // Kafka was unpaused in @Order(2) finally block
        ResponseEntity<Map> response = rest.exchange(
                "/api/v1/transactions", HttpMethod.POST,
                transactionRequest("chaos-kafka-recovery"), Map.class);

        assertThat(response.getStatusCode())
                .as("Expected 202 after Kafka recovery")
                .isEqualTo(HttpStatus.ACCEPTED);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private HttpEntity<Map<String, Object>> transactionRequest(String key) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(testJwt);
        Map<String, Object> body = Map.of(
                "idempotencyKey",  key,
                "userId",          "chaos-user-1",
                "amount",          "250.00",
                "currency",        "USD",
                "country",         "US",
                "deviceFingerprint", "fp-chaos"
        );
        return new HttpEntity<>(body, headers);
    }

    /**
     * Generates an HS256 JWT signed with the dev secret.
     * Relies on nimbus-jose-jwt (transitive dep of spring-boot-starter-oauth2-resource-server).
     */
    private static String generateTestJwt() throws Exception {
        byte[] keyBytes = "dev-jwt-secret-changeme-minimum-32-bytes!!"
                .getBytes(StandardCharsets.UTF_8);
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("chaos-test")
                .issuer("transaction-shield-test")
                .expirationTime(new Date(System.currentTimeMillis() + 86_400_000L))
                .build();
        SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
        jwt.sign(new MACSigner(keyBytes));
        return jwt.serialize();
    }
}
