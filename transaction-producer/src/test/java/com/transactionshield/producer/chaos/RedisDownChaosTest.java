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
 * Chaos Test: Redis Failure
 *
 * Scenario: The Redis instance becomes unavailable during operation.
 * Redis is used for idempotency SET NX. When it goes down:
 *   - {@link com.transactionshield.producer.service.IdempotencyService#tryAcquire} throws
 *     {@code RedisConnectionFailureException} (extends {@code DataAccessException})
 *   - {@link com.transactionshield.producer.exception.GlobalExceptionHandler#handleDataAccessFailure}
 *     catches it → 503 Service Unavailable
 *
 * Design principle: fail-closed is safer than fail-open for idempotency guards
 * (preventing double-processing during Redis flapping is preferable to allowing
 * unguarded writes that could cause duplicates when Redis recovers).
 *
 * Run:
 *   mvn test -pl transaction-producer -Dgroups=chaos
 */
@Tag("chaos")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
@Import(TestProducerKafkaConfig.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Chaos: Redis idempotency store failure")
class RedisDownChaosTest {

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
        // Short Redis timeout so chaos test fails fast instead of waiting 2s
        registry.add("spring.data.redis.timeout", () -> "300ms");
    }

    @Autowired
    TestRestTemplate rest;

    @Test
    @Order(1)
    @DisplayName("[1] Redis UP → transaction accepted (202)")
    void baseline_redisUp_returns202() {
        ResponseEntity<Map> response = rest.exchange(
                "/api/v1/transactions", HttpMethod.POST,
                transactionRequest("chaos-redis-baseline"), Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }

    @Test
    @Order(2)
    @DisplayName("[2] Redis DOWN → idempotency fails → 503 Service Unavailable")
    void chaos_redisDown_returns503() {
        REDIS.getDockerClient().pauseContainerCmd(REDIS.getContainerId()).exec();

        try {
            ResponseEntity<Map> response = rest.exchange(
                    "/api/v1/transactions", HttpMethod.POST,
                    transactionRequest("chaos-redis-down"), Map.class);

            assertThat(response.getStatusCode())
                    .as("Expected 503 when Redis is unavailable")
                    .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);

            assertThat(response.getBody())
                    .as("Response body should explain the failure")
                    .containsEntry("status", 503);
        } finally {
            REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec();
        }
    }

    @Test
    @Order(3)
    @DisplayName("[3] Redis RECOVERED → same idempotencyKey reusable (lock was never set)")
    void recovery_redisRestored_keyReusable() {
        // The key from test [2] was never acquired (Redis was down)
        // so the same key should succeed once Redis is back
        ResponseEntity<Map> response = rest.exchange(
                "/api/v1/transactions", HttpMethod.POST,
                transactionRequest("chaos-redis-down"), Map.class);

        assertThat(response.getStatusCode())
                .as("Key must be reusable after Redis recovery — it was never locked")
                .isEqualTo(HttpStatus.ACCEPTED);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private HttpEntity<Map<String, Object>> transactionRequest(String key) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(testJwt);
        Map<String, Object> body = Map.of(
                "idempotencyKey",    key,
                "userId",            "chaos-redis-user",
                "amount",            "99.00",
                "currency",          "EUR",
                "country",           "DE",
                "deviceFingerprint", "fp-redis-chaos"
        );
        return new HttpEntity<>(body, headers);
    }

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
