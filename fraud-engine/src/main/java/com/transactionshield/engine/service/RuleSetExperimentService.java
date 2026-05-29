package com.transactionshield.engine.service;

import com.transactionshield.engine.entity.RuleSetExperiment;
import com.transactionshield.engine.repository.RuleSetExperimentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Manages the active A/B rule set experiment and routes traffic deterministically.
 *
 * Routing formula:
 *   abs(hash(transactionId)) % 100 < experiment.trafficPct → "EXPERIMENT"
 *   otherwise → "STABLE"
 *
 * The same transactionId always maps to the same variant, so retries and
 * replayed DLQ messages are scored consistently.
 *
 * Active experiment is cached in memory and refreshed at the same interval
 * as rule configs so operators see new experiments take effect quickly.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class RuleSetExperimentService {

    public static final String STABLE     = "STABLE";
    public static final String EXPERIMENT = "EXPERIMENT";

    private final RuleSetExperimentRepository experimentRepository;

    private volatile Optional<RuleSetExperiment> activeExperiment = Optional.empty();

    @Scheduled(fixedDelayString = "${app.rules.refresh-interval-ms:300000}", initialDelay = 0)
    @Transactional(readOnly = true)
    public void refresh() {
        activeExperiment = experimentRepository.findFirstByActiveTrue();
        activeExperiment.ifPresentOrElse(
                e -> log.info("[EXPERIMENT] Active: '{}' — {}% traffic to EXPERIMENT", e.getName(), e.getTrafficPct()),
                () -> log.debug("[EXPERIMENT] No active experiment")
        );
    }

    /**
     * Deterministically selects the variant for a given transactionId.
     * Returns "STABLE" when no experiment is active.
     */
    public String selectVariant(String transactionId) {
        return activeExperiment
                .filter(e -> bucket(transactionId) < e.getTrafficPct())
                .map(e -> EXPERIMENT)
                .orElse(STABLE);
    }

    public boolean hasActiveExperiment() {
        return activeExperiment.isPresent();
    }

    public Optional<RuleSetExperiment> getActiveExperiment() {
        return activeExperiment;
    }

    // ── Lifecycle management ───────────────────────────────────────────

    @Transactional
    public RuleSetExperiment create(String name, String description, int trafficPct, String createdBy) {
        RuleSetExperiment experiment = RuleSetExperiment.builder()
                .name(name)
                .description(description)
                .trafficPct(trafficPct)
                .active(false)
                .createdAt(Instant.now())
                .createdBy(createdBy)
                .build();
        RuleSetExperiment saved = experimentRepository.save(experiment);
        log.info("[EXPERIMENT] Created '{}' — {}% traffic, created_by={}", name, trafficPct, createdBy);
        return saved;
    }

    @Transactional
    public RuleSetExperiment activate(java.util.UUID id) {
        // Deactivate any currently running experiment first
        experimentRepository.findFirstByActiveTrue().ifPresent(current -> {
            current.deactivate();
            experimentRepository.save(current);
            log.info("[EXPERIMENT] Deactivated previous experiment '{}'", current.getName());
        });

        RuleSetExperiment experiment = experimentRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Experiment not found: " + id));
        experiment.activate();
        RuleSetExperiment saved = experimentRepository.save(experiment);

        refresh(); // update in-memory cache immediately
        log.info("[EXPERIMENT] Activated '{}' — {}% traffic routed to EXPERIMENT", saved.getName(), saved.getTrafficPct());
        return saved;
    }

    @Transactional
    public RuleSetExperiment deactivate(java.util.UUID id) {
        RuleSetExperiment experiment = experimentRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Experiment not found: " + id));
        experiment.deactivate();
        RuleSetExperiment saved = experimentRepository.save(experiment);

        refresh();
        log.info("[EXPERIMENT] Deactivated '{}' — all traffic restored to STABLE", saved.getName());
        return saved;
    }

    @Transactional(readOnly = true)
    public List<RuleSetExperiment> listAll() {
        return experimentRepository.findAll();
    }

    // abs(hash) % 100 → bucket 0–99
    private static int bucket(String transactionId) {
        return Math.abs(transactionId.hashCode()) % 100;
    }
}
