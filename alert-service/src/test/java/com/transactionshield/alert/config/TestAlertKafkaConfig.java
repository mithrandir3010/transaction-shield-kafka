package com.transactionshield.alert.config;

import com.transactionshield.common.event.AlertCreatedEvent;
import com.transactionshield.common.event.ScoredTransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

/**
 * alert-service integration testleri için Kafka konfigürasyonu.
 *
 * Bean isimlendirme (fraud-engine TestKafkaConfig pattern'ini izler):
 *   ConsumerFactory  bean adı ≠ ContainerFactory bean adı
 *   → BeanDefinitionOverrideException'dan kaçınır.
 *
 *   alertCreatedEventConsumerFactory → ConsumerFactory<String, AlertCreatedEvent>
 *   testAlertCreatedConsumerFactory  → ConcurrentKafkaListenerContainerFactory (AlertCreatedEventCollector)
 *
 *   dlqStringEventConsumerFactory   → ConsumerFactory<String, String>
 *   testDlqConsumerFactory          → ConcurrentKafkaListenerContainerFactory (DlqMessageCollector)
 */
@TestConfiguration
public class TestAlertKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ── Test Producer — ScoredTransactionEvent → transactions.scored ──

    @Bean
    KafkaTemplate<String, ScoredTransactionEvent> scoredEventTemplate() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                JsonSerializer.ADD_TYPE_INFO_HEADERS,         false
        )));
    }

    // ── Test Consumer — alerts.created ────────────────────────────────

    @Bean
    ConsumerFactory<String, AlertCreatedEvent> alertCreatedEventConsumerFactory() {
        JsonDeserializer<AlertCreatedEvent> deserializer =
                new JsonDeserializer<>(AlertCreatedEvent.class);
        deserializer.addTrustedPackages("com.transactionshield.common.event");
        deserializer.setUseTypeMapperForKey(false);

        return new DefaultKafkaConsumerFactory<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG,           "alert-created-test-consumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true
                ),
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, AlertCreatedEvent>
    testAlertCreatedConsumerFactory(
            ConsumerFactory<String, AlertCreatedEvent> alertCreatedEventConsumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, AlertCreatedEvent>();
        factory.setConsumerFactory(alertCreatedEventConsumerFactory);
        factory.setConcurrency(1);
        return factory;
    }

    // ── Test Consumer — transactions.dlq (raw string) ─────────────────

    @Bean
    ConsumerFactory<String, String> dlqStringEventConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG,           "dlq-test-consumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true
                ),
                new StringDeserializer(),
                new StringDeserializer()
        );
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> testDlqConsumerFactory(
            ConsumerFactory<String, String> dlqStringEventConsumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(dlqStringEventConsumerFactory);
        factory.setConcurrency(1);
        return factory;
    }

    // ── Topic Creation ─────────────────────────────────────────────────

    @Bean org.apache.kafka.clients.admin.NewTopic transactionsScored() {
        return TopicBuilder.name("transactions.scored").partitions(3).replicas(1).build();
    }

    @Bean org.apache.kafka.clients.admin.NewTopic alertsCreated() {
        return TopicBuilder.name("alerts.created").partitions(3).replicas(1).build();
    }

    @Bean org.apache.kafka.clients.admin.NewTopic transactionsDlq() {
        return TopicBuilder.name("transactions.dlq").partitions(1).replicas(1).build();
    }
}
