package com.transactionshield.alert.consumer;

import com.transactionshield.alert.entity.Alert;
import com.transactionshield.alert.producer.AlertEventProducer;
import com.transactionshield.alert.service.AlertService;
import com.transactionshield.common.avro.AvroMapper;
import com.transactionshield.common.enums.RiskLevel;
import com.transactionshield.common.event.ScoredTransactionEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("ScoredTransactionConsumer — Kafka consumer delegasyon testleri")
class ScoredTransactionConsumerTest {

    @Mock AlertService alertService;
    @Mock AlertEventProducer alertEventProducer;
    @Mock Acknowledgment ack;
    @InjectMocks ScoredTransactionConsumer consumer;

    @Test
    @DisplayName("HIGH risk → alert persist edilir + AlertCreatedEvent publish edilir + ack verilir")
    void consume_highRisk_persistsAlertAndPublishesEvent() throws Exception {
        ScoredTransactionEvent event = scoredEvent("tx-h", RiskLevel.HIGH, 80);
        Alert saved = savedAlert("tx-h");

        given(alertService.saveAlert(any(ScoredTransactionEvent.class))).willReturn(saved);
        given(alertService.shouldNotify(any(ScoredTransactionEvent.class))).willReturn(true);

        consumer.consume(AvroMapper.toAvro(event), "transactions.scored", 0, 42L, ack);

        verify(alertService).saveAlert(any(ScoredTransactionEvent.class));
        verify(alertEventProducer).publish(saved);
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("CRITICAL risk → alert persist edilir + AlertCreatedEvent publish edilir + ack verilir")
    void consume_criticalRisk_persistsAlertAndPublishesEvent() throws Exception {
        ScoredTransactionEvent event = scoredEvent("tx-c", RiskLevel.CRITICAL, 100);
        Alert saved = savedAlert("tx-c");

        given(alertService.saveAlert(any(ScoredTransactionEvent.class))).willReturn(saved);
        given(alertService.shouldNotify(any(ScoredTransactionEvent.class))).willReturn(true);

        consumer.consume(AvroMapper.toAvro(event), "transactions.scored", 1, 99L, ack);

        verify(alertEventProducer).publish(saved);
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("LOW risk → alert persist edilir, AlertCreatedEvent publish edilmez, ack verilir")
    void consume_lowRisk_persistsAlertButNoEventPublished() throws Exception {
        ScoredTransactionEvent event = scoredEvent("tx-l", RiskLevel.LOW, 10);

        given(alertService.saveAlert(any(ScoredTransactionEvent.class))).willReturn(savedAlert("tx-l"));
        given(alertService.shouldNotify(any(ScoredTransactionEvent.class))).willReturn(false);

        consumer.consume(AvroMapper.toAvro(event), "transactions.scored", 0, 1L, ack);

        verifyNoInteractions(alertEventProducer);
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("MEDIUM risk → alert persist edilir, AlertCreatedEvent publish edilmez, ack verilir")
    void consume_mediumRisk_persistsAlertButNoEventPublished() throws Exception {
        ScoredTransactionEvent event = scoredEvent("tx-m", RiskLevel.MEDIUM, 50);

        given(alertService.saveAlert(any(ScoredTransactionEvent.class))).willReturn(savedAlert("tx-m"));
        given(alertService.shouldNotify(any(ScoredTransactionEvent.class))).willReturn(false);

        consumer.consume(AvroMapper.toAvro(event), "transactions.scored", 0, 2L, ack);

        verifyNoInteractions(alertEventProducer);
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("AlertService persist başarısız → exception propagate edilir, ack verilmez")
    void consume_persistFails_exceptionPropagatedNoAck() {
        ScoredTransactionEvent event = scoredEvent("tx-fail", RiskLevel.HIGH, 80);
        given(alertService.saveAlert(any(ScoredTransactionEvent.class)))
                .willThrow(new RuntimeException("DB down"));

        assertThatThrownBy(() ->
                consumer.consume(AvroMapper.toAvro(event), "transactions.scored", 0, 3L, ack))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("DB down");

        verifyNoInteractions(ack);
        verifyNoInteractions(alertEventProducer);
    }

    @Test
    @DisplayName("AlertEventProducer publish başarısız → exception propagate edilir, ack verilmez")
    void consume_publishFails_exceptionPropagatedNoAck() throws Exception {
        ScoredTransactionEvent event = scoredEvent("tx-pub-fail", RiskLevel.HIGH, 80);
        Alert saved = savedAlert("tx-pub-fail");

        given(alertService.saveAlert(any(ScoredTransactionEvent.class))).willReturn(saved);
        given(alertService.shouldNotify(any(ScoredTransactionEvent.class))).willReturn(true);
        willThrow(new RuntimeException("Kafka down")).given(alertEventProducer).publish(saved);

        assertThatThrownBy(() ->
                consumer.consume(AvroMapper.toAvro(event), "transactions.scored", 0, 4L, ack))
                .isInstanceOf(RuntimeException.class);

        verify(ack, never()).acknowledge();
    }

    @Test
    @DisplayName("Ack sadece tam başarı sonrası çağrılır — save + publish sonrası")
    void consume_ackCalledAfterSuccessfulPersistAndPublish() throws Exception {
        ScoredTransactionEvent event = scoredEvent("tx-ack", RiskLevel.HIGH, 80);
        Alert saved = savedAlert("tx-ack");

        given(alertService.saveAlert(any(ScoredTransactionEvent.class))).willReturn(saved);
        given(alertService.shouldNotify(any(ScoredTransactionEvent.class))).willReturn(true);

        consumer.consume(AvroMapper.toAvro(event), "transactions.scored", 0, 5L, ack);

        // save → publish → ack sırası
        var inOrder = inOrder(alertService, alertEventProducer, ack);
        inOrder.verify(alertService).saveAlert(any(ScoredTransactionEvent.class));
        inOrder.verify(alertEventProducer).publish(saved);
        inOrder.verify(ack).acknowledge();
    }

    private ScoredTransactionEvent scoredEvent(String txId, RiskLevel level, int score) {
        return new ScoredTransactionEvent(
                txId, "idem-1", "user-1",
                BigDecimal.valueOf(100), "USD", "US", null,
                Instant.now(), score, score, level,
                List.of("HIGH_AMOUNT"), Instant.now()
        );
    }

    private Alert savedAlert(String txId) {
        return Alert.builder()
                .id(UUID.randomUUID()).transactionId(txId)
                .userId("user-1").fraudScore(80).riskLevel("HIGH")
                .triggeredRules("HIGH_AMOUNT").status("OPEN")
                .createdAt(Instant.now()).updatedAt(Instant.now()).build();
    }
}
