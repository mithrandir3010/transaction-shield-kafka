package com.transactionshield.producer.config;

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

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    /**
     * Batching window: wait up to lingerMs before sending accumulated records.
     * 0 = lowest latency (fire immediately); 5–20ms = better throughput under load.
     */
    @Value("${app.kafka.producer-linger-ms:5}")
    private int lingerMs;

    /** Max bytes per batch. 16KB default; increase to 64–128KB for high throughput. */
    @Value("${app.kafka.producer-batch-size:16384}")
    private int batchSize;

    /** snappy = good balance of CPU vs compression ratio; lz4 for lower CPU cost. */
    @Value("${app.kafka.producer-compression-type:snappy}")
    private String compressionType;

    @Bean
    public ProducerFactory<String, com.transactionshield.avro.TransactionEvent> transactionEventProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,                   bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,              KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,                  true);
        props.put(ProducerConfig.ACKS_CONFIG,                                "all");
        props.put(ProducerConfig.RETRIES_CONFIG,                             3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,      5);
        props.put(ProducerConfig.LINGER_MS_CONFIG,                           lingerMs);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,                          batchSize);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,                    compressionType);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,                       67_108_864L); // 64 MB
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, com.transactionshield.avro.TransactionEvent> kafkaTemplate(
            ProducerFactory<String, com.transactionshield.avro.TransactionEvent> factory) {
        return new KafkaTemplate<>(factory);
    }
}
