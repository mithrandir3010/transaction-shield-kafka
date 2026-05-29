package com.transactionshield.engine.consumer;

import com.transactionshield.common.dlq.DlqHeaders;
import com.transactionshield.common.dlq.ErrorCategory;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Observability consumer for the Dead Letter Queue.
 *
 * Responsibilities (monitoring only — no business logic):
 *   - Logs every DLQ arrival with taxonomy and exception metadata
 *   - Increments dlq.message.total counter (tagged by topic + error_category)
 *   - Emits ERROR-level log for FATAL messages (PagerDuty / alerting hook)
 *
 * Replay and quarantine are handled by alert-service's DlqReplayService
 * (operator-triggered via POST /api/v1/dlq/replay).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DlqConsumer {

    private final MeterRegistry meterRegistry;

    @KafkaListener(
            topics           = "${app.kafka.transactions-dlq-topic}",
            groupId          = "${spring.kafka.consumer.group-id}-dlq",
            containerFactory = "dlqListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        String originalTopic  = readHeader(record, DlqHeaders.ORIGINAL_TOPIC);
        String errorCategory  = readHeader(record, DlqHeaders.ERROR_CATEGORY);
        String exceptionClass = readHeader(record, DlqHeaders.EXCEPTION_FQCN);
        String exceptionMsg   = readHeader(record, DlqHeaders.EXCEPTION_MESSAGE);
        String replayCount    = readHeader(record, DlqHeaders.REPLAY_COUNT);

        meterRegistry.counter("dlq.message.total",
                "original_topic",  originalTopic != null  ? originalTopic  : "unknown",
                "error_category",  errorCategory != null  ? errorCategory  : "UNKNOWN"
        ).increment();

        boolean isFatal = ErrorCategory.FATAL.name().equals(errorCategory);

        if (isFatal) {
            log.error("""
                    [DLQ-FATAL] Unrecoverable — schema fix + redeploy required before replay
                      key           : {}
                      originalTopic : {}
                      replayCount   : {}
                      exception     : {} — {}
                    """,
                    record.key(), originalTopic, replayCount, exceptionClass, exceptionMsg);
        } else {
            log.warn("[DLQ] Dead letter — category={} key={} originalTopic={} replayCount={} exception={}",
                    errorCategory, record.key(), originalTopic, replayCount, exceptionClass);
        }

        ack.acknowledge();
    }

    private static String readHeader(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h != null ? new String(h.value(), StandardCharsets.UTF_8) : null;
    }
}
