package com.transactionshield.alert.dlq;

import com.transactionshield.alert.service.QuarantineService;
import com.transactionshield.common.dlq.DlqHeaders;
import com.transactionshield.common.dlq.ErrorCategory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Operator-triggered DLQ replay engine.
 *
 * Uses a stable consumer group ({@code dlq-replay-consumer-group}) with committed
 * offsets so each replay run only processes messages that arrived since the
 * previous run. Pass {@code fromBeginning=true} to re-process all DLQ history.
 *
 * Replay decision per message:
 *   FATAL  or replayCount ≥ maxReplayAttempts → quarantine (persist to PG)
 *   NON_RETRYABLE                              → skip (log only)
 *   TRANSIENT and count < max                 → re-publish to original topic
 *                                               with incremented x-replay-count
 *
 * Re-publication uses {@link ByteArrayDeserializer} + {@link ByteArrayDeserializer}
 * to preserve the original Avro encoding (magic byte + schema ID + payload)
 * without going through Schema Registry again.
 */
@Service
@Slf4j
public class DlqReplayService {

    private static final String REPLAY_GROUP = "dlq-replay-consumer-group";
    private static final int    SEND_TIMEOUT_SECONDS = 5;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.transactions-dlq-topic}")
    private String dlqTopic;

    @Value("${app.dlq.replay.max-replay-attempts:3}")
    private int maxReplayAttempts;

    @Value("${app.dlq.replay.poll-timeout-seconds:10}")
    private long pollTimeoutSeconds;

    private final KafkaTemplate<String, byte[]> replayTemplate;
    private final QuarantineService quarantineService;

    public DlqReplayService(
            @Qualifier("replayKafkaTemplate") KafkaTemplate<String, byte[]> replayTemplate,
            QuarantineService quarantineService) {
        this.replayTemplate = replayTemplate;
        this.quarantineService = quarantineService;
    }

    // ── Request / Result ─────────────────────────────────────────────

    public record DlqReplayRequest(
            int maxMessages,
            ErrorCategory errorCategoryFilter,  // null = process TRANSIENT only
            boolean dryRun,
            boolean fromBeginning
    ) {
        public DlqReplayRequest {
            if (maxMessages < 1 || maxMessages > 10_000) {
                throw new IllegalArgumentException("maxMessages must be 1–10 000");
            }
        }
    }

    public record DlqReplayResult(
            int messagesRead,
            int replayed,
            int quarantined,
            int skipped
    ) {}

    // ── Main replay loop ─────────────────────────────────────────────

    public DlqReplayResult replay(DlqReplayRequest request) {
        log.info("[DLQ-REPLAY] Starting — maxMessages={} filter={} dryRun={} fromBeginning={}",
                request.maxMessages(), request.errorCategoryFilter(),
                request.dryRun(), request.fromBeginning());

        int read = 0, replayed = 0, quarantined = 0, skipped = 0;

        try (KafkaConsumer<String, byte[]> consumer = buildConsumer()) {
            consumer.subscribe(List.of(dlqTopic));

            // Trigger partition assignment before seeking
            consumer.poll(Duration.ofSeconds(3));

            if (request.fromBeginning()) {
                consumer.seekToBeginning(consumer.assignment());
                log.info("[DLQ-REPLAY] Seeked to beginning — {} partition(s)",
                        consumer.assignment().size());
            }

            Duration pollTimeout = Duration.ofSeconds(pollTimeoutSeconds);

            while (read < request.maxMessages()) {
                ConsumerRecords<String, byte[]> batch = consumer.poll(pollTimeout);
                if (batch.isEmpty()) break; // no new messages

                Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();

                for (ConsumerRecord<String, byte[]> record : batch) {
                    if (read >= request.maxMessages()) break;
                    read++;

                    ErrorCategory category = parseCategory(record);
                    String originalTopic   = readHeader(record, DlqHeaders.ORIGINAL_TOPIC);
                    String exceptionClass  = readHeader(record, DlqHeaders.EXCEPTION_FQCN);
                    String exceptionMsg    = readHeader(record, DlqHeaders.EXCEPTION_MESSAGE);
                    int replayCount        = parseReplayCount(record);

                    String outcome = processRecord(
                            record, category, originalTopic, exceptionClass,
                            exceptionMsg, replayCount, request);

                    switch (outcome) {
                        case "replayed"    -> replayed++;
                        case "quarantined" -> quarantined++;
                        default            -> skipped++;
                    }

                    toCommit.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                }

                if (!toCommit.isEmpty()) {
                    consumer.commitSync(toCommit);
                }
            }

        } catch (Exception e) {
            log.error("[DLQ-REPLAY] Aborted after read={} replayed={} quarantined={}: {}",
                    read, replayed, quarantined, e.getMessage(), e);
            throw new DlqReplayException("DLQ replay failed", e);
        }

        log.info("[DLQ-REPLAY] Done — read={} replayed={} quarantined={} skipped={}",
                read, replayed, quarantined, skipped);
        return new DlqReplayResult(read, replayed, quarantined, skipped);
    }

    // ── Per-record decision ──────────────────────────────────────────

    private String processRecord(
            ConsumerRecord<String, byte[]> record,
            ErrorCategory category,
            String originalTopic,
            String exceptionClass,
            String exceptionMsg,
            int replayCount,
            DlqReplayRequest request) {

        // Category filter (null means "process TRANSIENT only")
        ErrorCategory filter = request.errorCategoryFilter();
        if (filter != null && filter != category) {
            log.debug("[DLQ-REPLAY] Skipping (category filter) — key={} category={}", record.key(), category);
            return "skipped";
        }

        boolean exceedsLimit = replayCount >= maxReplayAttempts;

        if (category == ErrorCategory.FATAL || exceedsLimit) {
            String reason = exceedsLimit
                    ? "exceeded max replay attempts (" + maxReplayAttempts + ")"
                    : "FATAL — schema/deserialization error";
            log.warn("[DLQ-REPLAY] Quarantine — key={} category={} replayCount={} reason={}",
                    record.key(), category, replayCount, reason);

            if (!request.dryRun()) {
                quarantineService.quarantine(
                        record.key(), originalTopic, category,
                        exceptionClass, exceptionMsg, replayCount);
            }
            return "quarantined";
        }

        if (category == ErrorCategory.NON_RETRYABLE) {
            log.info("[DLQ-REPLAY] Skip NON_RETRYABLE — key={} exception={}",
                    record.key(), exceptionClass);
            return "skipped";
        }

        // TRANSIENT: re-publish to original topic with incremented replay count
        String target = originalTopic != null ? originalTopic : dlqTopic;
        log.info("[DLQ-REPLAY] Replay — key={} → topic={} attempt={}", record.key(), target, replayCount + 1);

        if (!request.dryRun()) {
            publish(record, target, replayCount + 1);
        }
        return "replayed";
    }

    private void publish(ConsumerRecord<String, byte[]> record, String targetTopic, int newCount) {
        RecordHeaders headers = new RecordHeaders();
        for (Header h : record.headers()) {
            if (!DlqHeaders.REPLAY_COUNT.equals(h.key())) {
                headers.add(h); // preserve all original headers
            }
        }
        headers.add(new RecordHeader(
                DlqHeaders.REPLAY_COUNT,
                String.valueOf(newCount).getBytes(StandardCharsets.UTF_8)));

        ProducerRecord<String, byte[]> producerRecord =
                new ProducerRecord<>(targetTopic, null, record.key(), record.value(), headers);

        try {
            replayTemplate.send(producerRecord).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DlqReplayException("Replay interrupted for key=" + record.key(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new DlqReplayException("Replay send failed for key=" + record.key(), e);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────

    private KafkaConsumer<String, byte[]> buildConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,       bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                REPLAY_GROUP);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,       "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,      false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,        100);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,      30_000);
        return new KafkaConsumer<>(props);
    }

    private ErrorCategory parseCategory(ConsumerRecord<?, ?> record) {
        String value = readHeader(record, DlqHeaders.ERROR_CATEGORY);
        if (value == null) return ErrorCategory.TRANSIENT;
        try {
            return ErrorCategory.valueOf(value);
        } catch (IllegalArgumentException e) {
            return ErrorCategory.TRANSIENT;
        }
    }

    private int parseReplayCount(ConsumerRecord<?, ?> record) {
        String value = readHeader(record, DlqHeaders.REPLAY_COUNT);
        if (value == null) return 0;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String readHeader(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h != null ? new String(h.value(), StandardCharsets.UTF_8) : null;
    }

    public static class DlqReplayException extends RuntimeException {
        public DlqReplayException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
