package com.transactionshield.alert.controller;

import com.transactionshield.alert.dlq.DlqReplayService;
import com.transactionshield.alert.entity.QuarantinedMessage;
import com.transactionshield.alert.entity.QuarantineStatus;
import com.transactionshield.alert.service.QuarantineService;
import com.transactionshield.common.dlq.ErrorCategory;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * Operator API for DLQ management.
 *
 * Endpoints:
 *   POST  /api/v1/dlq/replay            — trigger controlled replay
 *   GET   /api/v1/dlq/quarantine        — list quarantined messages
 *   PATCH /api/v1/dlq/quarantine/{id}   — resolve or discard a quarantined message
 *
 * Example replay request (TRANSIENT only, dry-run first):
 *   POST /api/v1/dlq/replay
 *   { "maxMessages": 100, "dryRun": true, "fromBeginning": false }
 *
 * Example force-replay from beginning:
 *   { "maxMessages": 500, "fromBeginning": true }
 *
 * Example resolve:
 *   PATCH /api/v1/dlq/quarantine/550e8400-e29b-...
 *   { "resolvedBy": "ops@company.com", "notes": "Schema v2 deployed", "discard": false }
 */
@RestController
@RequestMapping("/api/v1/dlq")
@RequiredArgsConstructor
public class DlqReplayController {

    private final DlqReplayService replayService;
    private final QuarantineService quarantineService;

    public record ReplayRequest(
            int maxMessages,
            ErrorCategory errorCategoryFilter,
            boolean dryRun,
            boolean fromBeginning
    ) {}

    public record ResolveRequest(
            String resolvedBy,
            String notes,
            boolean discard
    ) {}

    @PostMapping("/replay")
    public ResponseEntity<DlqReplayService.DlqReplayResult> replay(@RequestBody ReplayRequest req) {
        int max = req.maxMessages() <= 0 ? 100 : req.maxMessages();
        var request = new DlqReplayService.DlqReplayRequest(
                max,
                req.errorCategoryFilter(),
                req.dryRun(),
                req.fromBeginning()
        );
        return ResponseEntity.ok(replayService.replay(request));
    }

    @GetMapping("/quarantine")
    public Page<QuarantinedMessage> listQuarantined(
            @RequestParam(required = false) QuarantineStatus status,
            Pageable pageable) {
        return quarantineService.list(status, pageable);
    }

    @PatchMapping("/quarantine/{id}")
    public ResponseEntity<QuarantinedMessage> resolve(
            @PathVariable UUID id,
            @RequestBody ResolveRequest req) {
        QuarantinedMessage updated = req.discard()
                ? quarantineService.discard(id, req.resolvedBy(), req.notes())
                : quarantineService.resolve(id, req.resolvedBy(), req.notes());
        return ResponseEntity.ok(updated);
    }
}
