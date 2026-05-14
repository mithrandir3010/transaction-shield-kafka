package com.transactionshield.alert.config;

import com.transactionshield.common.event.ScoredTransactionEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

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

    @Bean
    public ConsumerFactory<String, ScoredTransactionEvent> scoredEventConsumerFactory() {
        JsonDeserializer<ScoredTransactionEvent> deserializer =
                new JsonDeserializer<>(ScoredTransactionEvent.class);
        deserializer.addTrustedPackages("com.transactionshield.common.event");
        deserializer.setUseTypeMapperForKey(false);

        return new DefaultKafkaConsumerFactory<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG,           groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG,   10
                ),
                new StringDeserializer(),
                deserializer
        );
    }

    /**
     * Retry policy for the alert-service consumer:
     *  - 3 total attempts (1 original + 2 retries) with exponential backoff
     *  - DeserializationException: non-retryable → straight to DLQ
     *
     * Note: @Retryable on AlertService handles transient DB errors separately
     * (service-level retry) before this Kafka-level retry kicks in. The two
     * layers are independent: DB retries happen within a single Kafka attempt.
     */
    @Bean
    public DefaultErrorHandler alertErrorHandler(KafkaTemplate<String, Object> dlqKafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                dlqKafkaTemplate,
                (record, ex) -> {
                    log.error("Alert DLQ — topic={} key={} cause={}", dlqTopic, record.key(), ex.getMessage());
                    return new TopicPartition(dlqTopic, -1);
                }
        );

        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxAttempts(3);
        backOff.setMaxInterval(8_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(DeserializationException.class);
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ScoredTransactionEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, ScoredTransactionEvent> scoredEventConsumerFactory,
            DefaultErrorHandler alertErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, ScoredTransactionEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(scoredEventConsumerFactory);
        factory.setCommonErrorHandler(alertErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(3);
        return factory;
    }
}
