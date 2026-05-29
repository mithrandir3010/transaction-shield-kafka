package com.transactionshield.engine.repository;

import com.transactionshield.engine.entity.RuleSetExperiment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface RuleSetExperimentRepository extends JpaRepository<RuleSetExperiment, UUID> {

    Optional<RuleSetExperiment> findFirstByActiveTrue();
}
