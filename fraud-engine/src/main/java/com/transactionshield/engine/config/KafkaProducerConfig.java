package com.transactionshield.engine.config;

import com.transactionshield.common.event.ScoredTransactionEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Typed template for publishing ScoredTransactionEvent to transactions.scored.
     */
    @Bean
    public KafkaTemplate<String, ScoredTransactionEvent> scoredEventKafkaTemplate() {
        return new KafkaTemplate<>(scoredEventProducerFactory());
    }

    /**
     * Generic template used by DeadLetterPublishingRecoverer.
     * Sends raw failed ConsumerRecords (any value type) to transactions.dlq.
     * Qualifier "dlqKafkaTemplate" disambiguates from the typed scored template.
     */
    @Bean("dlqKafkaTemplate")
    public KafkaTemplate<String, Object> dlqKafkaTemplate() {
        return new KafkaTemplate<>(genericProducerFactory());
    }

    private ProducerFactory<String, ScoredTransactionEvent> scoredEventProducerFactory() {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,              bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,           StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,         JsonSerializer.class,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,             true,
                ProducerConfig.ACKS_CONFIG,                           "all",
                ProducerConfig.RETRIES_CONFIG,                        3,
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5,
                JsonSerializer.ADD_TYPE_INFO_HEADERS,                 false
        ));
    }

    private ProducerFactory<String, Object> genericProducerFactory() {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                JsonSerializer.ADD_TYPE_INFO_HEADERS,         false
        ));
    }
}
