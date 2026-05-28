package com.transactionshield.producer.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("IdempotencyService — Redis SET NX davranışı")
class IdempotencyServiceTest {

    @Mock StringRedisTemplate redisTemplate;
    @Mock ValueOperations<String, String> valueOps;
    @InjectMocks IdempotencyService idempotencyService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(idempotencyService, "ttlHours", 24L);
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOps);
    }

    @Test
    @DisplayName("setIfAbsent TRUE döndürünce lock alınır")
    void tryAcquire_newKey_returnsTrue() {
        given(valueOps.setIfAbsent(
                eq("idempotency:transaction:key-1"),
                eq("PROCESSING"),
                any(Duration.class))
        ).willReturn(Boolean.TRUE);

        assertThat(idempotencyService.tryAcquire("key-1")).isTrue();
    }

    @Test
    @DisplayName("setIfAbsent FALSE döndürünce duplicate tespit edilir")
    void tryAcquire_existingKey_returnsFalse() {
        given(valueOps.setIfAbsent(any(), any(), any())).willReturn(Boolean.FALSE);

        assertThat(idempotencyService.tryAcquire("key-dup")).isFalse();
    }

    @Test
    @DisplayName("setIfAbsent NULL döndürünce (Redis timeout) false döner")
    void tryAcquire_redisReturnsNull_returnsFalse() {
        given(valueOps.setIfAbsent(any(), any(), any())).willReturn(null);

        assertThat(idempotencyService.tryAcquire("key-null")).isFalse();
    }

    @Test
    @DisplayName("release() doğru Redis key'ini siler")
    void release_deletesCorrectRedisKey() {
        idempotencyService.release("key-to-release");

        verify(redisTemplate).delete("idempotency:transaction:key-to-release");
    }

    @Test
    @DisplayName("tryAcquire() Redis key prefix'i doğru kullanmalı")
    void tryAcquire_usesCorrectKeyPrefix() {
        given(valueOps.setIfAbsent(any(), any(), any())).willReturn(Boolean.TRUE);

        idempotencyService.tryAcquire("my-key");

        verify(valueOps).setIfAbsent(
                eq("idempotency:transaction:my-key"),
                eq("PROCESSING"),
                eq(Duration.ofHours(24))
        );
    }
}
