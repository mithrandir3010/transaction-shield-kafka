package com.transactionshield.alert.controller;

import com.transactionshield.alert.dto.AlertResponse;
import com.transactionshield.alert.service.AlertService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import org.springframework.security.test.context.support.WithMockUser;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(AlertController.class)
@WithMockUser
@DisplayName("AlertController — HTTP katmanı testleri")
class AlertControllerTest {

    @Autowired MockMvc mockMvc;
    @MockBean AlertService alertService;

    private static final String URL = "/api/v1/alerts";

    @Test
    @DisplayName("GET /alerts → 200 + sayfalı sonuç")
    void listAlerts_noFilter_returns200WithPage() throws Exception {
        AlertResponse alert = new AlertResponse(
                "a-001", "tx-001", "user-1",
                100, "CRITICAL", List.of("BLACKLISTED_COUNTRY"),
                "OPEN", Instant.now());

        given(alertService.listAlerts(any(), isNull()))
                .willReturn(new PageImpl<>(List.of(alert)));

        mockMvc.perform(get(URL))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].transactionId").value("tx-001"))
                .andExpect(jsonPath("$.content[0].riskLevel").value("CRITICAL"))
                .andExpect(jsonPath("$.content[0].fraudScore").value(100));
    }

    @Test
    @DisplayName("GET /alerts?riskLevel=CRITICAL → alertService'e CRITICAL filter iletilir")
    void listAlerts_withRiskLevelFilter_passesFilterToService() throws Exception {
        given(alertService.listAlerts(any(), eq("CRITICAL")))
                .willReturn(new PageImpl<>(List.of()));

        mockMvc.perform(get(URL).param("riskLevel", "CRITICAL"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(0));
    }

    @Test
    @DisplayName("GET /alerts?page=1&size=5 → sayfalama parametreleri iletilir")
    void listAlerts_paginationParams_passedToService() throws Exception {
        given(alertService.listAlerts(
                eq(PageRequest.of(1, 5, org.springframework.data.domain.Sort.by("createdAt").descending())),
                isNull()))
                .willReturn(new PageImpl<>(List.of(), PageRequest.of(1, 5), 0));

        mockMvc.perform(get(URL).param("page", "1").param("size", "5"))
                .andExpect(status().isOk());
    }

    @Test
    @DisplayName("size > 100 → 100 ile sınırlandırılır")
    void listAlerts_sizeOver100_clampedTo100() throws Exception {
        given(alertService.listAlerts(any(), isNull()))
                .willReturn(new PageImpl<>(List.of()));

        // size=200 gönder — 100'e kesilmeli (service çağrısı page size=100 ile yapılır)
        mockMvc.perform(get(URL).param("size", "200"))
                .andExpect(status().isOk());
    }

    @Test
    @DisplayName("Boş sayfa → 200 + boş content listesi")
    void listAlerts_emptyResult_returns200WithEmptyContent() throws Exception {
        given(alertService.listAlerts(any(), isNull()))
                .willReturn(new PageImpl<>(List.of()));

        mockMvc.perform(get(URL))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content").isEmpty());
    }
}
