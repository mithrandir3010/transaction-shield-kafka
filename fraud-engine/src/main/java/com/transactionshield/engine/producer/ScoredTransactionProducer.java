package com.transactionshield.engine.producer;

import com.transactionshield.common.avro.AvroMapper;
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

    private final KafkaTemplate<String, com.transactionshield.avro.ScoredTransactionEvent> scoredEventKafkaTemplate;

    @Value("${app.kafka.transactions-scored-topic}")
    private String scoredTopic;

    @Value("${app.kafka.publish-timeout-seconds:5}")
    private long publishTimeoutSeconds;

    public void publish(ScoredTransactionEvent event) throws Exception {
        scoredEventKafkaTemplate
                .send(scoredTopic, event.transactionId(), AvroMapper.toAvro(event))
                .get(publishTimeoutSeconds, TimeUnit.SECONDS);

        log.info("Scored event published — transactionId={} fraudScore={} riskLevel={} topic={}",
                event.transactionId(), event.fraudScore(), event.riskLevel(), scoredTopic);
    }
}
