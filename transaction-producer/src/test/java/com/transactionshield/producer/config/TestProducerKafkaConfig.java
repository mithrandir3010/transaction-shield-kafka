package com.transactionshield.producer.config;

import com.transactionshield.common.event.TransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

/**
 * Test-özel Kafka consumer factory — transactions.raw'dan TransactionEvent okur.
 *
 * Bean isimlendirme:
 *   rawTransactionConsumerFactory   → ConsumerFactory<String, TransactionEvent>
 *   testRawConsumerFactory          → ConcurrentKafkaListenerContainerFactory (RawEventCollector'ın beklediği)
 *
 * Neden iki ayrı isim?
 *   fraud-engine TestKafkaConfig'in izlediği patern:
 *   ConsumerFactory bean adı ≠ ContainerFactory bean adı.
 *   Aynı isim kullanılırsa Spring BeanDefinitionOverrideException fırlatır.
 */
@TestConfiguration
public class TestProducerKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    ConsumerFactory<String, TransactionEvent> rawTransactionConsumerFactory() {
        JsonDeserializer<TransactionEvent> deserializer =
                new JsonDeserializer<>(TransactionEvent.class);
        deserializer.addTrustedPackages("com.transactionshield.common.event");
        deserializer.setUseTypeMapperForKey(false);

        return new DefaultKafkaConsumerFactory<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG,           "producer-test-consumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG,   10
                ),
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> testRawConsumerFactory(
            ConsumerFactory<String, TransactionEvent> rawTransactionConsumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, TransactionEvent>();
        factory.setConsumerFactory(rawTransactionConsumerFactory);
        factory.setConcurrency(1);
        return factory;
    }

    @Bean
    org.apache.kafka.clients.admin.NewTopic transactionsRaw() {
        return TopicBuilder.name("transactions.raw").partitions(3).replicas(1).build();
    }
}
