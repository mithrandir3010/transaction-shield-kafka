package com.transactionshield.alert.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Internal API key authentication filter for Alert Service.
 *
 * Protects /api/v1/alerts/** and /api/v1/dlq/** with X-Internal-Api-Key header.
 * Unauthenticated rejection is handled by Spring Security's AuthorizationFilter.
 */
@Component
@RequiredArgsConstructor
public class ApiKeyAuthenticationFilter extends OncePerRequestFilter {

    static final String API_KEY_HEADER = "X-Internal-Api-Key";

    @Value("${app.security.internal-api-key}")
    private String validApiKey;

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain chain)
            throws ServletException, IOException {

        String receivedKey = request.getHeader(API_KEY_HEADER);
        if (StringUtils.hasText(receivedKey) && receivedKey.equals(validApiKey)) {
            SecurityContextHolder.getContext()
                    .setAuthentication(new ApiKeyAuthenticationToken(receivedKey));
        }
        chain.doFilter(request, response);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return request.getServletPath().startsWith("/actuator");
    }
}
