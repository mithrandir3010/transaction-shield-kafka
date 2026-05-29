package com.transactionshield.engine.controller;

import com.transactionshield.engine.entity.FraudRuleEntity;
import com.transactionshield.engine.entity.RuleSetExperiment;
import com.transactionshield.engine.entity.ScoringAuditLog;
import com.transactionshield.engine.repository.FraudRuleRepository;
import com.transactionshield.engine.rule.RuleConfig;
import com.transactionshield.engine.service.AuditLogService;
import com.transactionshield.engine.service.RuleConfigService;
import com.transactionshield.engine.service.RuleSetExperimentService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Operator API for rule engine management.
 *
 * Rules:
 *   GET  /api/v1/rules                          → list all cached rule configs
 *   PUT  /api/v1/rules/{ruleCode}/{variant}      → update weight + parameters in DB
 *   POST /api/v1/rules/refresh                  → force reload from DB (no-wait cache bust)
 *
 * Experiments (A/B):
 *   GET  /api/v1/rules/experiments              → list all experiments
 *   POST /api/v1/rules/experiments              → create new experiment
 *   POST /api/v1/rules/experiments/{id}/activate   → start A/B routing
 *   POST /api/v1/rules/experiments/{id}/deactivate → stop, restore to STABLE
 *   POST /api/v1/rules/experiments/{id}/promote    → copy EXPERIMENT configs to STABLE
 *
 * Audit:
 *   GET  /api/v1/rules/audit?transactionId=...  → scoring history for a transaction
 *   GET  /api/v1/rules/audit?variant=...        → all scorings for a variant (paged)
 */
@RestController
@RequestMapping("/api/v1/rules")
@RequiredArgsConstructor
public class RuleManagementController {

    private final RuleConfigService        ruleConfigService;
    private final RuleSetExperimentService experimentService;
    private final AuditLogService          auditLogService;
    private final FraudRuleRepository      fraudRuleRepository;

    // ── Rule config ───────────────────────────────────────────────────

    @GetMapping
    public List<RuleConfig> listRules() {
        return ruleConfigService.getAllConfigs();
    }

    @PutMapping("/{ruleCode}/{variant}")
    @Transactional
    public ResponseEntity<FraudRuleEntity> updateRule(
            @PathVariable String ruleCode,
            @PathVariable String variant,
            @RequestBody UpdateRuleRequest req) {

        FraudRuleEntity entity = fraudRuleRepository
                .findByRuleCodeAndVariant(ruleCode, variant)
                .orElseGet(() -> FraudRuleEntity.builder()
                        .ruleCode(ruleCode)
                        .variant(variant)
                        .createdAt(Instant.now())
                        .build());

        entity.setScoreWeight(req.scoreWeight());
        entity.setEnabled(req.enabled());
        entity.setDescription(req.description());
        entity.setParameters(req.parameters());
        entity.setUpdatedAt(Instant.now());

        FraudRuleEntity saved = fraudRuleRepository.save(entity);
        ruleConfigService.refresh(); // propagate immediately
        return ResponseEntity.ok(saved);
    }

    @PostMapping("/refresh")
    public ResponseEntity<Map<String, Object>> forceRefresh() {
        ruleConfigService.refresh();
        experimentService.refresh();
        return ResponseEntity.ok(Map.of(
                "status", "refreshed",
                "configs", ruleConfigService.getAllConfigs().size(),
                "activeExperiment", experimentService.getActiveExperiment()
                        .map(RuleSetExperiment::getName).orElse("none")
        ));
    }

    // ── Experiments ───────────────────────────────────────────────────

    @GetMapping("/experiments")
    public List<RuleSetExperiment> listExperiments() {
        return experimentService.listAll();
    }

    @PostMapping("/experiments")
    public ResponseEntity<RuleSetExperiment> createExperiment(
            @RequestBody CreateExperimentRequest req) {
        RuleSetExperiment created = experimentService.create(
                req.name(), req.description(), req.trafficPct(), req.createdBy());
        return ResponseEntity.ok(created);
    }

    @PostMapping("/experiments/{id}/activate")
    public ResponseEntity<RuleSetExperiment> activateExperiment(@PathVariable UUID id) {
        return ResponseEntity.ok(experimentService.activate(id));
    }

    @PostMapping("/experiments/{id}/deactivate")
    public ResponseEntity<RuleSetExperiment> deactivateExperiment(@PathVariable UUID id) {
        return ResponseEntity.ok(experimentService.deactivate(id));
    }

    /**
     * Promotes all EXPERIMENT rule configs to STABLE by overwriting weights+params.
     * Deactivates the experiment after promotion.
     */
    @PostMapping("/experiments/{id}/promote")
    @Transactional
    public ResponseEntity<Map<String, Object>> promoteExperiment(@PathVariable UUID id) {
        experimentService.deactivate(id);

        List<FraudRuleEntity> experimentRules = fraudRuleRepository.findByVariant("EXPERIMENT");
        int promoted = 0;
        for (FraudRuleEntity exp : experimentRules) {
            FraudRuleEntity stable = fraudRuleRepository
                    .findByRuleCodeAndVariant(exp.getRuleCode(), "STABLE")
                    .orElseGet(() -> FraudRuleEntity.builder()
                            .ruleCode(exp.getRuleCode())
                            .variant("STABLE")
                            .createdAt(Instant.now())
                            .build());

            stable.setScoreWeight(exp.getScoreWeight());
            stable.setEnabled(exp.isEnabled());
            stable.setParameters(exp.getParameters());
            stable.setDescription(exp.getDescription());
            stable.setUpdatedAt(Instant.now());
            fraudRuleRepository.save(stable);
            promoted++;
        }

        ruleConfigService.refresh();
        return ResponseEntity.ok(Map.of("promoted", promoted, "status", "STABLE updated"));
    }

    // ── Audit log ─────────────────────────────────────────────────────

    @GetMapping("/audit")
    public Object getAuditLog(
            @RequestParam(required = false) String transactionId,
            @RequestParam(required = false) String variant,
            Pageable pageable) {

        if (transactionId != null) {
            return auditLogService.findByTransactionId(transactionId);
        }
        if (variant != null) {
            return auditLogService.findByVariant(variant, pageable);
        }
        return auditLogService.findByVariant("STABLE", pageable);
    }

    // ── Request DTOs ──────────────────────────────────────────────────

    public record UpdateRuleRequest(
            int scoreWeight,
            boolean enabled,
            String description,
            Map<String, String> parameters
    ) {}

    public record CreateExperimentRequest(
            String name,
            String description,
            int trafficPct,
            String createdBy
    ) {}
}
