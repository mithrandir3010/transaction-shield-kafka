package com.transactionshield.engine.security;

import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.List;

/** Immutable token representing a verified internal-service caller. */
final class ApiKeyAuthenticationToken extends AbstractAuthenticationToken {

    private final String apiKey;

    ApiKeyAuthenticationToken(String apiKey) {
        super(List.of(new SimpleGrantedAuthority("ROLE_INTERNAL_SERVICE")));
        this.apiKey = apiKey;
        setAuthenticated(true);
    }

    @Override
    public Object getCredentials() {
        return apiKey;
    }

    @Override
    public Object getPrincipal() {
        return "internal-service";
    }
}
