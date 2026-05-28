package com.transactionshield.engine.consumer;

import com.transactionshield.common.avro.AvroMapper;
import com.transactionshield.engine.service.FraudEngineService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionEventConsumer {

    private final FraudEngineService fraudEngineService;

    @KafkaListener(
            topics           = "${app.kafka.transactions-raw-topic}",
            groupId          = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload  com.transactionshield.avro.TransactionEvent avroEvent,
            @Header(KafkaHeaders.RECEIVED_TOPIC)     String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int    partition,
            @Header(KafkaHeaders.OFFSET)             long   offset,
            Acknowledgment ack) throws Exception {

        log.info("Received — transactionId={} topic={} partition={} offset={}",
                avroEvent.getTransactionId(), topic, partition, offset);

        fraudEngineService.process(AvroMapper.fromAvro(avroEvent));

        ack.acknowledge();
        log.debug("Offset committed — partition={} offset={}", partition, offset);
    }
}
