package com.transactionshield.alert.consumer;

import com.transactionshield.alert.entity.Alert;
import com.transactionshield.alert.producer.AlertEventProducer;
import com.transactionshield.alert.service.AlertService;
import com.transactionshield.common.avro.AvroMapper;
import com.transactionshield.common.event.ScoredTransactionEvent;
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
public class ScoredTransactionConsumer {

    private final AlertService       alertService;
    private final AlertEventProducer alertEventProducer;

    @KafkaListener(
            topics           = "${app.kafka.transactions-scored-topic}",
            groupId          = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload  com.transactionshield.avro.ScoredTransactionEvent avroEvent,
            @Header(KafkaHeaders.RECEIVED_TOPIC)     String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int    partition,
            @Header(KafkaHeaders.OFFSET)             long   offset,
            Acknowledgment ack) throws Exception {

        ScoredTransactionEvent event = AvroMapper.fromAvro(avroEvent);

        log.info("ScoredTransaction received — transactionId={} fraudScore={} riskLevel={} offset={}",
                event.transactionId(), event.fraudScore(), event.riskLevel(), offset);

        Alert saved = alertService.saveAlert(event);

        if (alertService.shouldNotify(event)) {
            log.info("Risk level {} triggers notification — transactionId={}",
                    event.riskLevel(), event.transactionId());
            alertEventProducer.publish(saved);
        }

        ack.acknowledge();
        log.debug("Offset committed — partition={} offset={}", partition, offset);
    }
}
