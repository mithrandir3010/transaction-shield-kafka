package com.transactionshield.producer.config;

import com.transactionshield.common.event.TransactionEvent;
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

    @Bean
    public ProducerFactory<String, TransactionEvent> transactionEventProducerFactory() {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,                bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,             StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,           JsonSerializer.class,

                // Exactly-once delivery guarantee at the producer level.
                // Requires: acks=all, retries>0, max.in.flight<=5
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,               true,
                ProducerConfig.ACKS_CONFIG,                             "all",
                ProducerConfig.RETRIES_CONFIG,                          3,
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,   5,

                // Do NOT embed Java type info in headers — consumers will
                // deserialize using the known TransactionEvent schema.
                JsonSerializer.ADD_TYPE_INFO_HEADERS,                   false
        ));
    }

    @Bean
    public KafkaTemplate<String, TransactionEvent> kafkaTemplate(
            ProducerFactory<String, TransactionEvent> factory) {
        return new KafkaTemplate<>(factory);
    }
}
