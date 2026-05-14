package com.transactionshield.engine.config;

import com.transactionshield.common.event.ScoredTransactionEvent;
import com.transactionshield.common.event.TransactionEvent;
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
 * Test-özel Kafka konfigürasyonu.
 *
 * Bu sınıf @TestConfiguration ile işaretlidir → sadece test context'inde aktif olur,
 * üretim kod'una karışmaz. AbstractIntegrationTest'te @Import ile dahil edilir.
 *
 * İçerik:
 *   1. rawEventTemplate   — TransactionEvent'i transactions.raw'a yayan test producer'ı
 *   2. testKafkaListenerContainerFactory — ScoredTransactionEvent'leri okuyan test consumer factory'si
 *   3. NewTopic bean'leri — Testcontainers Kafka başladığında topic'leri otomatik oluşturur
 */
@TestConfiguration
public class TestKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ── Test Producer ─────────────────────────────────────────────────

    /**
     * TransactionEvent'i transactions.raw topic'ine yazar.
     * fraud-engine'in üretim KafkaTemplate'leri ScoredTransactionEvent veya Object
     * için tanımlıdır; test için TransactionEvent'e özel bir bean gereklidir.
     */
    @Bean
    KafkaTemplate<String, TransactionEvent> rawEventTemplate() {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                JsonSerializer.ADD_TYPE_INFO_HEADERS,         false
        )));
    }

    // ── Test Consumer Factory ─────────────────────────────────────────

    /**
     * ScoredTransactionEvent'leri transactions.scored'dan okuyan ayrı bir consumer factory.
     *
     * Neden ayrı factory?
     *   fraud-engine'in kafkaListenerContainerFactory'si TransactionEvent için yapılandırılmış.
     *   ScoredTransactionEvent'i deserialize etmek için ayrı bir JsonDeserializer gerekir.
     *   Bean adı "testKafkaListenerContainerFactory" → ScoredEventCollector'da containerFactory
     *   parametresiyle seçilir, üretim factory'siyle çakışma olmaz.
     */
    @Bean
    ConsumerFactory<String, ScoredTransactionEvent> testScoredConsumerFactory() {
        JsonDeserializer<ScoredTransactionEvent> deserializer =
                new JsonDeserializer<>(ScoredTransactionEvent.class);
        deserializer.addTrustedPackages("com.transactionshield.common.event");
        deserializer.setUseTypeMapperForKey(false);

        return new DefaultKafkaConsumerFactory<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG,           "integration-test-consumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true,  // test consumer: auto-commit yeterli
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG,   10
                ),
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, ScoredTransactionEvent>
    testKafkaListenerContainerFactory(ConsumerFactory<String, ScoredTransactionEvent> testScoredConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, ScoredTransactionEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(testScoredConsumerFactory);
        factory.setConcurrency(1); // test'te tek thread yeterli ve deterministik
        return factory;
    }

    // ── Topic Creation ────────────────────────────────────────────────
    // KafkaAdmin bu bean'leri context başlarken Testcontainers Kafka'ya uygular.
    // Üretim docker-compose'daki kafka-init servisinin yaptığını test ortamında bu bean'ler yapar.

    @Bean org.apache.kafka.clients.admin.NewTopic transactionsRaw() {
        return TopicBuilder.name("transactions.raw").partitions(3).replicas(1).build();
    }

    @Bean org.apache.kafka.clients.admin.NewTopic transactionsScored() {
        return TopicBuilder.name("transactions.scored").partitions(3).replicas(1).build();
    }

    @Bean org.apache.kafka.clients.admin.NewTopic transactionsDlq() {
        return TopicBuilder.name("transactions.dlq").partitions(1).replicas(1).build();
    }
}
