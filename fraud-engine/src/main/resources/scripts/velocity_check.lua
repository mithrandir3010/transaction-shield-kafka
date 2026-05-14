--[[
  Velocity Check — Sliding Window Algorithm (Atomik)
  =====================================================
  Redis ZSET kullanarak her kullanıcı için gerçek zamanlı
  kayan pencere (sliding window) işlem sayacı.

  Neden bu script atomik?
    Redis, Lua scriptlerini tek thread üzerinde çalıştırır.
    Başka hiçbir komut bu 4 operasyon arasına giremez.
    Bu, Pipeline'ın sağlayamadığı gerçek atomikliği garanti eder.

  ZSET yapısı:
    key     : "velocity:user:<userId>"
    score   : işlem zamanı (epoch milliseconds)
    member  : transactionId (unique → aynı tx iki kez sayılmaz)

  KEYS[1]  : Redis key  — "velocity:user:<userId>"
  ARGV[1]  : nowMs      — şimdiki zaman (epoch ms, string)
  ARGV[2]  : windowMs   — pencere boyutu ms cinsinden (örn. 60000 = 1 dk)
  ARGV[3]  : member     — transactionId (ZSET üyesi, benzersiz)
  ARGV[4]  : ttlSec     — key için TTL saniye cinsinden

  Returns  : Pencere içindeki toplam işlem sayısı (Long)
--]]

local key      = KEYS[1]
local nowMs    = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local member   = ARGV[3]
local ttlSec   = tonumber(ARGV[4])

-- [1] Yeni işlemi ZSET'e ekle (score = şimdiki timestamp)
--     Aynı member zaten varsa sadece score güncellenir (duplicate safe)
redis.call('ZADD', key, nowMs, member)

-- [2] Pencere dışına çıkan eski girişleri temizle
--     score < (now - window) olan tüm üyeleri sil
redis.call('ZREMRANGEBYSCORE', key, '-inf', nowMs - windowMs)

-- [3] Kayan penceredeki aktif işlem sayısını say
local count = redis.call('ZCARD', key)

-- [4] Key'in TTL'ini yenile (pencere boyutunun 2 katı — güvenlik marjı)
--     Son işlemden windowMs*2 sonra key otomatik silinir
redis.call('EXPIRE', key, ttlSec)

return count
