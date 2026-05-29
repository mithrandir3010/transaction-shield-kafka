package com.transactionshield.engine.repository;

import com.transactionshield.engine.entity.FraudRuleEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface FraudRuleRepository extends JpaRepository<FraudRuleEntity, UUID> {

    List<FraudRuleEntity> findByVariant(String variant);

    Optional<FraudRuleEntity> findByRuleCodeAndVariant(String ruleCode, String variant);

    List<FraudRuleEntity> findAll();
}
