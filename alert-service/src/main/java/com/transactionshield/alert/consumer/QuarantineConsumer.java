package com.transactionshield.alert.consumer;

import com.transactionshield.alert.service.QuarantineService;
import com.transactionshield.common.dlq.DlqHeaders;
import com.transactionshield.common.dlq.ErrorCategory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Auto-quarantine consumer for the Dead Letter Queue.
 *
 * Listens to transactions.dlq with a dedicated consumer group so it receives
 * every DLQ message independently of other consumers (fraud-engine's DlqConsumer,
 * test collectors, etc.).
 *
 * Auto-quarantine policy:
 *   FATAL messages → persist to quarantined_messages immediately (schema broken)
 *   TRANSIENT/NON_RETRYABLE → ack only; replay-service handles these on demand
 *
 * FATAL messages are quarantined here so operators see them in the quarantine
 * view without having to trigger a manual replay run.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class QuarantineConsumer {

    private final QuarantineService quarantineService;

    @KafkaListener(
            topics           = "${app.kafka.transactions-dlq-topic}",
            groupId          = "${spring.kafka.consumer.group-id}-dlq-monitor",
            containerFactory = "dlqListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        String errorCategory  = readHeader(record, DlqHeaders.ERROR_CATEGORY);
        String originalTopic  = readHeader(record, DlqHeaders.ORIGINAL_TOPIC);
        String exceptionClass = readHeader(record, DlqHeaders.EXCEPTION_FQCN);
        String exceptionMsg   = readHeader(record, DlqHeaders.EXCEPTION_MESSAGE);
        String replayCountStr = readHeader(record, DlqHeaders.REPLAY_COUNT);
        int replayCount = parseIntOrZero(replayCountStr);

        if (ErrorCategory.FATAL.name().equals(errorCategory)) {
            log.error("[QUARANTINE-CONSUMER] Auto-quarantining FATAL message — key={} topic={} exception={}",
                    record.key(), originalTopic, exceptionClass);

            quarantineService.quarantine(
                    record.key(),
                    originalTopic,
                    ErrorCategory.FATAL,
                    exceptionClass,
                    exceptionMsg,
                    replayCount
            );
        }

        ack.acknowledge();
    }

    private static String readHeader(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h != null ? new String(h.value(), StandardCharsets.UTF_8) : null;
    }

    private static int parseIntOrZero(String value) {
        if (value == null) return 0;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
