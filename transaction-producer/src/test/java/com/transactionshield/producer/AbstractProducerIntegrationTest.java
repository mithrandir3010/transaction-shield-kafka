package com.transactionshield.producer;

import com.transactionshield.producer.config.TestProducerKafkaConfig;
import com.transactionshield.producer.support.RawEventCollector;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

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
@Testcontainers
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

    static {
        Startables.deepStart(KAFKA, REDIS).join();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host",         REDIS::getHost);
        registry.add("spring.data.redis.port",         () -> REDIS.getMappedPort(6379));
    }

    @Autowired protected TestRestTemplate restTemplate;
    @Autowired protected RawEventCollector collector;

    @BeforeEach
    void clearCollector() {
        collector.clear();
    }
}
