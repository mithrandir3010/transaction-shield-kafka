package com.transactionshield.producer;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.transactionshield.producer.config.TestProducerKafkaConfig;
import com.transactionshield.producer.support.RawEventCollector;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
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

/**
 * transaction-producer integration testleri için temel sınıf.
 *
 * Konteyner:
 *   - Kafka (Testcontainers)  → transactions.raw
 *   - Redis (Testcontainers)  → idempotency SET NX
 *
 * Paralel başlatma: Startables.deepStart() ile Kafka + Redis eş zamanlı başlar.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
@Import(TestProducerKafkaConfig.class)
public abstract class AbstractProducerIntegrationTest {

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

    static {
        try {
            if (DockerClientFactory.instance().isDockerAvailable()) {
                Startables.deepStart(KAFKA, REDIS).join();
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
        registry.add("spring.data.redis.host",         REDIS::getHost);
        registry.add("spring.data.redis.port",         () -> REDIS.getMappedPort(6379));
    }

    /** HS256 JWT signed with the dev secret — valid for 24h, reused across all tests. */
    static String TEST_JWT;

    static {
        try {
            byte[] key = "dev-jwt-secret-changeme-minimum-32-bytes!!"
                    .getBytes(StandardCharsets.UTF_8);
            JWTClaimsSet claims = new JWTClaimsSet.Builder()
                    .subject("integration-test")
                    .issuer("transaction-shield-test")
                    .expirationTime(new Date(System.currentTimeMillis() + 86_400_000L))
                    .build();
            SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
            jwt.sign(new MACSigner(key));
            TEST_JWT = jwt.serialize();
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate test JWT", e);
        }
    }

    @Autowired protected TestRestTemplate restTemplate;
    @Autowired protected RawEventCollector collector;

    @BeforeEach
    void clearCollector() {
        collector.clear();
    }

    /** Wraps a request body with Authorization: Bearer header for JWT-protected endpoints. */
    protected <T, R> ResponseEntity<R> postWithAuth(String url, T body, Class<R> responseType) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(TEST_JWT);
        headers.set("Content-Type", "application/json");
        return restTemplate.postForEntity(url, new HttpEntity<>(body, headers), responseType);
    }
}
