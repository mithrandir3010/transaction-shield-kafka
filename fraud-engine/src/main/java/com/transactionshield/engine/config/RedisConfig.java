package com.transactionshield.engine.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;

@Configuration
public class RedisConfig {

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }

    /**
     * Velocity check Lua script bean — yükleme startup'ta bir kez yapılır.
     *
     * DefaultRedisScript, scripti SHA1 hash'i ile Redis'e yükler (SCRIPT LOAD).
     * Sonraki çağrılar EVALSHA kullanır: ağ üzerinden script metni gitmez,
     * sadece hash → daha az bant genişliği, daha hızlı çalışma.
     *
     * ResultType Long: Lua'dan dönen integer → Java Long olarak map'lenir.
     */
    @Bean
    public DefaultRedisScript<Long> velocityScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptSource(
                new ResourceScriptSource(new ClassPathResource("scripts/velocity_check.lua"))
        );
        script.setResultType(Long.class);
        return script;
    }
}
