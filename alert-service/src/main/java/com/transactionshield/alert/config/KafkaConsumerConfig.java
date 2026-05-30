package com.transactionshield.alert.config;

import com.transactionshield.alert.dlq.ErrorClassifier;
import com.transactionshield.common.dlq.DlqHeaders;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.ExponentialBackOff;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${app.kafka.transactions-dlq-topic}")
    private String dlqTopic;

    @Value("${app.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Value("${app.kafka.consumer-concurrency:3}")
    private int consumerConcurrency;

    @Value("${app.kafka.max-poll-records:100}")
    private int maxPollRecords;

    // ── Main consumer — typed for ScoredTransactionEvent (Avro) ───────
    @Bean
    public ConsumerFactory<String, com.transactionshield.avro.ScoredTransactionEvent> scoredEventConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,                   bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                            groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,                   "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,                  false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,                    maxPollRecords);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,                     1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,                   100);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,              StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,            KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,   true);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ── DLQ consumer — raw bytes (handles mixed message types) ────────
    @Bean
    public ConsumerFactory<String, byte[]> dlqConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class
                // group.id set per-listener via @KafkaListener.groupId
        ));
    }

    // ── Error handler with enriched DLQ headers ────────────────────────
    @Bean
    public DefaultErrorHandler alertErrorHandler(KafkaTemplate<String, Object> dlqKafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                dlqKafkaTemplate,
                (record, ex) -> {
                    log.error("Alert DLQ — topic={} key={} category={} cause={}",
                            dlqTopic, record.key(), ErrorClassifier.classify(ex), ex.getMessage());
                    return new TopicPartition(dlqTopic, -1);
                }
        );

        // Enrich DLQ messages with error taxonomy and replay tracking headers
        recoverer.setHeadersFunction((record, ex) -> {
            RecordHeaders headers = new RecordHeaders();
            headers.add(new RecordHeader(
                    DlqHeaders.ERROR_CATEGORY,
                    ErrorClassifier.classify(ex).name().getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader(
                    DlqHeaders.REPLAY_COUNT,
                    "0".getBytes(StandardCharsets.UTF_8)));
            return headers;
        });

        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxAttempts(3);
        backOff.setMaxInterval(8_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        // FATAL — bypass retry, route to DLQ immediately
        handler.addNotRetryableExceptions(DeserializationException.class);
        // NON_RETRYABLE — duplicate, replaying will always fail
        handler.addNotRetryableExceptions(DataIntegrityViolationException.class);
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }

    // ── Listener container factories ───────────────────────────────────
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, com.transactionshield.avro.ScoredTransactionEvent>
    kafkaListenerContainerFactory(
            ConsumerFactory<String, com.transactionshield.avro.ScoredTransactionEvent> scoredEventConsumerFactory,
            DefaultErrorHandler alertErrorHandler) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, com.transactionshield.avro.ScoredTransactionEvent>();
        factory.setConsumerFactory(scoredEventConsumerFactory);
        factory.setCommonErrorHandler(alertErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(consumerConcurrency);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]>
    dlqListenerContainerFactory(ConsumerFactory<String, byte[]> dlqConsumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, byte[]>();
        factory.setConsumerFactory(dlqConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(1); // single-partition DLQ topic
        return factory;
    }
}
