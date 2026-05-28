package com.transactionshield.producer.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.Map;

@TestConfiguration
public class TestProducerKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Bean
    ConsumerFactory<String, com.transactionshield.avro.TransactionEvent> rawTransactionConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,                    bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG,                             "producer-test-consumer",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,                    "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,                   true,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG,                     10,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,               StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,             KafkaAvroDeserializer.class,
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,  schemaRegistryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,    true
        ));
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, com.transactionshield.avro.TransactionEvent>
    testRawConsumerFactory(
            ConsumerFactory<String, com.transactionshield.avro.TransactionEvent> rawTransactionConsumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, com.transactionshield.avro.TransactionEvent>();
        factory.setConsumerFactory(rawTransactionConsumerFactory);
        factory.setConcurrency(1);
        return factory;
    }

    @Bean
    org.apache.kafka.clients.admin.NewTopic transactionsRaw() {
        return TopicBuilder.name("transactions.raw").partitions(3).replicas(1).build();
    }
}
