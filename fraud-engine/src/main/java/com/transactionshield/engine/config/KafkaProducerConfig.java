package com.transactionshield.engine.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Bean
    public KafkaTemplate<String, com.transactionshield.avro.ScoredTransactionEvent> scoredEventKafkaTemplate() {
        return new KafkaTemplate<>(scoredEventProducerFactory());
    }

    @Bean("dlqKafkaTemplate")
    public KafkaTemplate<String, Object> dlqKafkaTemplate() {
        return new KafkaTemplate<>(genericProducerFactory());
    }

    private ProducerFactory<String, com.transactionshield.avro.ScoredTransactionEvent> scoredEventProducerFactory() {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,                bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,             StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,           KafkaAvroSerializer.class,
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,               true,
                ProducerConfig.ACKS_CONFIG,                             "all",
                ProducerConfig.RETRIES_CONFIG,                          3,
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,   5
        ));
    }

    private ProducerFactory<String, Object> genericProducerFactory() {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,                bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,             StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,           KafkaAvroSerializer.class,
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
        ));
    }
}
