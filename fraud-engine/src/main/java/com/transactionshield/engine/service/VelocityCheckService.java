package com.transactionshield.engine.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Redis Sorted Set tabanlı Sliding Window implementasyonu.
 *
 * ZSET yapısı (key: "velocity:user:<userId>"):
 *
 *   member (üye)  → transactionId  (benzersiz, duplicate-safe)
 *   score         → epoch milliseconds  (zamana göre sıralama)
 *
 * Neden member olarak transactionId kullanıyoruz?
 *   Kafka at-least-once garantisi ile aynı event iki kez gelebilir.
 *   ZADD ile aynı member tekrar eklenirse sadece score güncellenir,
 *   yeni bir eleman oluşmaz → işlem iki kez sayılmaz.
 *
 * Neden basit INCR+TTL yerine ZSET?
 *   INCR + TTL → fixed window (her dakika başında sıfırlanır)
 *   ZSET       → true sliding window (sürekli kayan, dakika sınırında sıçrama yok)
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class VelocityCheckService {

    private static final String KEY_PREFIX = "velocity:user:";

    private final StringRedisTemplate        redisTemplate;
    private final DefaultRedisScript<Long>   velocityScript;

    /**
     * Mevcut işlemi pencereye ekler ve penceredeki toplam sayıyı döner.
     *
     * Bu çağrı Lua script aracılığıyla Redis'te 4 komutu atomik çalıştırır:
     *   ZADD → ZREMRANGEBYSCORE → ZCARD → EXPIRE
     *
     * @param userId        Kullanıcı ID'si (key namespace için)
     * @param transactionId Mevcut işlem ID'si (ZSET üyesi)
     * @param windowMs      Pencere boyutu (milisaniye, örn. 60_000 = 1 dk)
     * @param ttlSeconds    Redis key TTL (saniye, tipik olarak windowMs/1000 * 2)
     * @return Son windowMs milisaniyedeki işlem sayısı (mevcut işlem dahil)
     */
    public int countInWindow(String userId, String transactionId,
                              long windowMs, long ttlSeconds) {
        String key   = KEY_PREFIX + userId;
        long   nowMs = Instant.now().toEpochMilli();

        // execute(script, keys, args...) → tek network round-trip, EVALSHA ile
        Long count = redisTemplate.execute(
                velocityScript,
                List.of(key),                    // KEYS[1]
                String.valueOf(nowMs),           // ARGV[1] — şimdiki zaman
                String.valueOf(windowMs),        // ARGV[2] — pencere boyutu
                transactionId,                   // ARGV[3] — ZSET üyesi
                String.valueOf(ttlSeconds)       // ARGV[4] — TTL
        );

        int result = (count != null) ? count.intValue() : 1;

        log.debug("Velocity — userId={} windowMs={} count={} key={}",
                userId, windowMs, result, key);

        return result;
    }

    /**
     * Belirli bir kullanıcının mevcut penceredeki işlem sayısını
     * ZSET'e ekleme yapmadan okur (read-only, test/debug için).
     */
    public long peekCurrentCount(String userId, long windowMs) {
        String key   = KEY_PREFIX + userId;
        long   nowMs = Instant.now().toEpochMilli();
        long   cutOff = nowMs - windowMs;

        Long count = redisTemplate.opsForZSet().count(key, cutOff, nowMs);
        return count != null ? count : 0;
    }
}
