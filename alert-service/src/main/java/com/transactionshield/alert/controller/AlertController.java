package com.transactionshield.alert.controller;

import com.transactionshield.alert.dto.AlertResponse;
import com.transactionshield.alert.service.AlertService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/alerts")
@RequiredArgsConstructor
@Slf4j
public class AlertController {

    private final AlertService alertService;

    /**
     * Lists persisted alerts, newest first.
     *
     * Query params:
     *   page      — zero-based page index (default 0)
     *   size      — page size, max 100 (default 20)
     *   riskLevel — optional filter: LOW | MEDIUM | HIGH | CRITICAL
     *
     * Examples:
     *   GET /api/v1/alerts
     *   GET /api/v1/alerts?riskLevel=CRITICAL
     *   GET /api/v1/alerts?riskLevel=HIGH&page=1&size=10
     */
    @GetMapping
    public ResponseEntity<Page<AlertResponse>> listAlerts(
            @RequestParam(defaultValue = "0")  int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false)    String riskLevel) {

        int clampedSize = Math.min(size, 100);  // prevent unbounded queries
        var pageable = PageRequest.of(page, clampedSize, Sort.by("createdAt").descending());

        log.debug("Listing alerts — page={} size={} riskLevel={}", page, clampedSize, riskLevel);

        Page<AlertResponse> result = alertService.listAlerts(pageable, riskLevel);
        return ResponseEntity.ok(result);
    }
}
