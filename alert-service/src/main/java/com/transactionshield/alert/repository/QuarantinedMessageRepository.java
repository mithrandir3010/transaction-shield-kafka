package com.transactionshield.alert.repository;

import com.transactionshield.alert.entity.QuarantinedMessage;
import com.transactionshield.alert.entity.QuarantineStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface QuarantinedMessageRepository extends JpaRepository<QuarantinedMessage, UUID> {

    Optional<QuarantinedMessage> findByTransactionIdAndOriginalTopic(String transactionId, String originalTopic);

    Page<QuarantinedMessage> findByStatus(QuarantineStatus status, Pageable pageable);
}
