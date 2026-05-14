package com.transactionshield.engine.config;

import com.transactionshield.common.event.TransactionEvent;
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
    public ConsumerFactory<String, TransactionEvent> consumerFactory() {
        JsonDeserializer<TransactionEvent> deserializer = new JsonDeserializer<>(TransactionEvent.class);
        // Trust only our known event package — reject anything else
        deserializer.addTrustedPackages("com.transactionshield.common.event");
        deserializer.setUseTypeMapperForKey(false);

        return new DefaultKafkaConsumerFactory<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG,           groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest",
                        // Manual ack: commit offset only AFTER successful processing
                        // Prevents message loss if the service crashes mid-processing
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG,   10
                ),
                new StringDeserializer(),
                deserializer
        );
    }

    /**
     * DLQ recovery strategy:
     *  - Exponential backoff: 1s → 2s → 4s (3 total attempts)
     *  - After 3 failures → publish raw ConsumerRecord to transactions.dlq
     *  - DeserializationException → skip retries, go straight to DLQ
     *
     * DeadLetterPublishingRecoverer preserves the original message headers
     * (including exception details) so DLQ consumers can inspect root cause.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> dlqKafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                dlqKafkaTemplate,
                (record, ex) -> {
                    log.error("Publishing to DLQ — topic={} key={} cause={}",
                            dlqTopic, record.key(), ex.getMessage());
                    return new TopicPartition(dlqTopic, -1); // -1 = Kafka selects partition
                }
        );

        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxAttempts(3);
        backOff.setMaxInterval(8_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // Deserialization failures are not retryable — bad payload won't fix itself
        handler.addNotRetryableExceptions(DeserializationException.class);

        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, TransactionEvent> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        // MANUAL_IMMEDIATE: ack() call in the listener commits immediately
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(3); // 1 thread per partition
        return factory;
    }
}
