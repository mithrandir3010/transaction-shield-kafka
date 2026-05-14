package com.transactionshield.engine.producer;

import com.transactionshield.common.event.ScoredTransactionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class ScoredTransactionProducer {

    private final KafkaTemplate<String, ScoredTransactionEvent> scoredEventKafkaTemplate;

    @Value("${app.kafka.transactions-scored-topic}")
    private String scoredTopic;

    @Value("${app.kafka.publish-timeout-seconds:5}")
    private long publishTimeoutSeconds;

    /**
     * Publishes the scored event synchronously.
     * Using transactionId as the Kafka partition key ensures all events
     * for the same transaction land on the same partition (ordering guarantee).
     *
     * @throws Exception propagated to the consumer, triggering the DLQ error handler
     */
    public void publish(ScoredTransactionEvent event) throws Exception {
        scoredEventKafkaTemplate
                .send(scoredTopic, event.transactionId(), event)
                .get(publishTimeoutSeconds, TimeUnit.SECONDS);

        log.info("Scored event published — transactionId={} fraudScore={} riskLevel={} topic={}",
                event.transactionId(), event.fraudScore(), event.riskLevel(), scoredTopic);
    }
}
