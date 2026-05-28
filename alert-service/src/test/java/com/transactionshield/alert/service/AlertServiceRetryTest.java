package com.transactionshield.alert.service;

import com.transactionshield.alert.config.RetryConfig;
import com.transactionshield.alert.exception.AlertPersistenceException;
import com.transactionshield.alert.repository.AlertRepository;
import com.transactionshield.common.enums.RiskLevel;
import com.transactionshield.common.event.ScoredTransactionEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @Retryable davranış testi — Spring AOP proxy'si üzerinden gerçek retry akışını doğrular.
 *
 * @DataJpaTest: H2 in-memory datasource + TransactionManager sağlar.
 * @Import: AlertService ve RetryConfig bean'lerini context'e ekler.
 * @MockBean AlertRepository: gerçek DB yerine kontrollü hata üretir.
 *
 * Neden bu yaklaşım?
 *   @Retryable, Spring AOP proxy'sine ihtiyaç duyar.
 *   @ExtendWith(MockitoExtension) bunu sağlamaz.
 *   @DataJpaTest ise Kafka/Redis olmadan minimal Spring context kurar.
 */
@DataJpaTest
@Import({AlertService.class, RetryConfig.class})
@TestPropertySource(properties = "app.alert.notify-risk-levels=HIGH,CRITICAL")
@DisplayName("AlertService @Retryable — Spring Retry davranış testleri")
class AlertServiceRetryTest {

    @MockBean AlertRepository alertRepository;
    @Autowired AlertService alertService;

    @Test
    @DisplayName("DataAccessException → 3 deneme yapılır (1 orijinal + 2 retry)")
    void saveAlert_transientDbError_retriesThreeTimes() {
        given(alertRepository.findByTransactionId(any())).willReturn(Optional.empty());
        given(alertRepository.save(any())).willThrow(transientDbException());

        assertThatThrownBy(() -> alertService.saveAlert(buildEvent("tx-retry-3")))
                .isInstanceOf(AlertPersistenceException.class)
                .hasMessageContaining("tx-retry-3");

        // maxAttempts=3 → 3 kez save() çağrısı
        verify(alertRepository, times(3)).save(any());
    }

    @Test
    @DisplayName("DataIntegrityViolationException → retry yapılmaz (1 deneme)")
    void saveAlert_uniqueConstraintViolation_noRetry() {
        given(alertRepository.findByTransactionId(any())).willReturn(Optional.empty());
        given(alertRepository.save(any()))
                .willThrow(new DataIntegrityViolationException("unique_constraint"));

        assertThatThrownBy(() -> alertService.saveAlert(buildEvent("tx-no-retry")))
                .isInstanceOf(DataIntegrityViolationException.class);

        // noRetryFor=DataIntegrityViolationException → sadece 1 deneme
        verify(alertRepository, times(1)).save(any());
    }

    @Test
    @DisplayName("İlk deneme başarısız, ikinci deneme başarılı — 2 kez save() çağrısı")
    void saveAlert_failsOnceThenSucceeds_twoSaveAttempts() {
        given(alertRepository.findByTransactionId(any())).willReturn(Optional.empty());

        // İlk çağrıda hata, ikincisinde başarı
        given(alertRepository.save(any()))
                .willThrow(transientDbException())
                .willAnswer(inv -> inv.getArgument(0));

        alertService.saveAlert(buildEvent("tx-retry-ok"));

        verify(alertRepository, times(2)).save(any());
    }

    @Test
    @DisplayName("@Recover — tüm denemeler başarısız → AlertPersistenceException wraps root cause")
    void saveAlert_allRetriesExhausted_recoverWrapsException() {
        DataAccessException rootCause = transientDbException();
        given(alertRepository.findByTransactionId(any())).willReturn(Optional.empty());
        given(alertRepository.save(any())).willThrow(rootCause);

        assertThatThrownBy(() -> alertService.saveAlert(buildEvent("tx-recover")))
                .isInstanceOf(AlertPersistenceException.class)
                .hasCause(rootCause);
    }

    private ScoredTransactionEvent buildEvent(String txId) {
        return new ScoredTransactionEvent(
                txId, "idem-1", "user-1",
                BigDecimal.valueOf(500), "USD", "US", null,
                Instant.now(), 80, 80, RiskLevel.HIGH,
                List.of("HIGH_AMOUNT"), Instant.now()
        );
    }

    private DataAccessException transientDbException() {
        return new DataAccessException("Transient DB error") {};
    }
}
