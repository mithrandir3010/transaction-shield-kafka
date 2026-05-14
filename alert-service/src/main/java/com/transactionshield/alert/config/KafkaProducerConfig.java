package com.transactionshield.alert.config;

import com.transactionshield.common.event.AlertCreatedEvent;
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

    /** Typed template for alerts.created topic. */
    @Bean
    public KafkaTemplate<String, AlertCreatedEvent> alertCreatedKafkaTemplate() {
        return new KafkaTemplate<>(alertCreatedProducerFactory());
    }

    /** Generic template for DLQ (used by DeadLetterPublishingRecoverer). */
    @Bean("dlqKafkaTemplate")
    public KafkaTemplate<String, Object> dlqKafkaTemplate() {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                JsonSerializer.ADD_TYPE_INFO_HEADERS,         false
        );
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    private ProducerFactory<String, AlertCreatedEvent> alertCreatedProducerFactory() {
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
}
