package com.transactionshield.alert.producer;

import com.transactionshield.alert.entity.Alert;
import com.transactionshield.common.avro.AvroMapper;
import com.transactionshield.common.event.AlertCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class AlertEventProducer {

    private final KafkaTemplate<String, com.transactionshield.avro.AlertCreatedEvent> alertCreatedKafkaTemplate;

    @Value("${app.kafka.alerts-created-topic}")
    private String alertsCreatedTopic;

    @Value("${app.kafka.publish-timeout-seconds:5}")
    private long publishTimeoutSeconds;

    public void publish(Alert alert) throws Exception {
        List<String> rules = (alert.getTriggeredRules() != null && !alert.getTriggeredRules().isBlank())
                ? Arrays.asList(alert.getTriggeredRules().split(","))
                : List.of();

        AlertCreatedEvent event = new AlertCreatedEvent(
                alert.getId().toString(),
                alert.getTransactionId(),
                alert.getUserId(),
                alert.getFraudScore(),
                alert.getRiskLevel(),
                rules,
                Instant.now()
        );

        alertCreatedKafkaTemplate
                .send(alertsCreatedTopic, alert.getTransactionId(), AvroMapper.toAvro(event))
                .get(publishTimeoutSeconds, TimeUnit.SECONDS);

        log.info("AlertCreatedEvent published — alertId={} transactionId={} riskLevel={} topic={}",
                alert.getId(), alert.getTransactionId(), alert.getRiskLevel(), alertsCreatedTopic);
    }
}
