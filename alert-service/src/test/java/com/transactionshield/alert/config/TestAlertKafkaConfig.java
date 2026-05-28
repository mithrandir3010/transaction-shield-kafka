package com.transactionshield.alert.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
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

import java.util.Map;

@TestConfiguration
public class TestAlertKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    // ── Test Producer — ScoredTransactionEvent → transactions.scored ──

    @Bean
    KafkaTemplate<String, com.transactionshield.avro.ScoredTransactionEvent> scoredEventTemplate() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,                bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,             StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,           KafkaAvroSerializer.class,
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
        )));
    }

    // ── Test Consumer — alerts.created ────────────────────────────────

    @Bean
    ConsumerFactory<String, com.transactionshield.avro.AlertCreatedEvent> alertCreatedEventConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,                    bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG,                             "alert-created-test-consumer",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,                    "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,                   true,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,               StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,             KafkaAvroDeserializer.class,
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,  schemaRegistryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,    true
        ));
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, com.transactionshield.avro.AlertCreatedEvent>
    testAlertCreatedConsumerFactory(
            ConsumerFactory<String, com.transactionshield.avro.AlertCreatedEvent> alertCreatedEventConsumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, com.transactionshield.avro.AlertCreatedEvent>();
        factory.setConsumerFactory(alertCreatedEventConsumerFactory);
        factory.setConcurrency(1);
        return factory;
    }

    // ── Test Consumer — transactions.dlq (raw bytes → String) ─────────

    @Bean
    ConsumerFactory<String, String> dlqStringEventConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                java.util.Map.of(
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
