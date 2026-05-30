# Runbook: Redis Down

**Severity:** P1 — customer-facing impact (transaction-producer returns 503)  
**Services affected:** transaction-producer (idempotency), fraud-engine (velocity checks, scoring idempotency)  
**On-call rotation:** Platform / Infrastructure  
**RTO:** 5 min (Redis restart) | 15 min (fail-over to replica)  
**RPO:** 0 — Redis stores ephemeral idempotency locks (TTL 24h); no durable data loss  

---

## What is this alert?

Redis becomes unreachable. Immediate consequences:
1. `transaction-producer` → `IdempotencyService.tryAcquire()` throws `RedisConnectionFailureException`
   → HTTP 503 on every POST `/api/v1/transactions`
2. `fraud-engine` → `ScoringIdempotencyService` throws → consumer error handler routes to DLQ
   → TRANSIENT DLQ spike (see [DLQ_SPIKE runbook](./DLQ_SPIKE.md))
3. `fraud-engine` → `VelocityCheckService` (Redis ZSET) throws → velocity rule skipped
   → High-velocity fraud may pass undetected during outage

Prometheus rule:
```
up{job="redis"} == 0  for 1m
```
Or via Spring Boot actuator:
```
http_server_requests_seconds_count{uri="/actuator/health",status="503"} > 0
```

---

## 1 — Immediate triage (≤ 3 min)

```bash
# 1. Confirm Redis is down:
redis-cli -h ${REDIS_HOST:-localhost} -p ${REDIS_PORT:-6379} ping
# Expected: PONG — if timeout or "Could not connect": Redis is unreachable

# 2. Check Docker / process status:
docker inspect ts-redis --format='{{.State.Status}}'
# or on ECS: aws ecs describe-tasks ...

# 3. Check Redis memory (OOM eviction?):
redis-cli INFO memory | grep -E "used_memory_human|maxmemory_human|mem_allocator"

# 4. Check Redis logs:
docker logs ts-redis --tail 50

# 5. Check actuator health on both services:
curl -s http://localhost:8082/actuator/health | jq '.components.redis'
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
       http://localhost:8083/actuator/health | jq '.components.redis'
```

---

## 2 — Diagnosis

### Scenario A: Redis OOM (memory exhausted)
```bash
redis-cli INFO memory | grep used_memory_human
# If near maxmemory (256mb in docker-compose):

# Check eviction policy (should be allkeys-lru):
redis-cli CONFIG GET maxmemory-policy

# Check key count and largest keys:
redis-cli DBSIZE
redis-cli --bigkeys --scan
```
**Fix:** Increase `maxmemory` or investigate unbounded key growth.
Idempotency keys have 24h TTL; velocity ZSET keys should auto-expire.

### Scenario B: Redis crash / OOM kill
```bash
# Check system OOM killer:
dmesg | grep -i "oom\|killed" | tail -10
# On EC2/ECS: check CloudWatch logs for OOMKilled events
```

### Scenario C: Network partition (Redis reachable from host but not from app)
```bash
# Test from within the app container:
docker exec ts-fraud-engine sh -c "nc -zv ${REDIS_HOST} ${REDIS_PORT}"
# Check DNS resolution:
docker exec ts-fraud-engine sh -c "nslookup ${REDIS_HOST}"
```

### Scenario D: Redis evicted idempotency keys (allkeys-lru under memory pressure)
```bash
# Check evicted_keys counter (non-zero = keys were force-evicted):
redis-cli INFO stats | grep evicted_keys
# If > 0 under allkeys-lru: idempotency keys may have been evicted prematurely
# → duplicate transactions possible during the pressure window
```

---

## 3 — Remediation

### Fast path: restart Redis
```bash
# Docker Compose:
docker compose restart redis

# Wait for ready:
until redis-cli -h localhost ping 2>/dev/null | grep -q PONG; do
  echo "Waiting for Redis..."; sleep 2
done
echo "Redis is up"

# ECS: force new task deployment
aws ecs update-service \
  --cluster transaction-shield \
  --service redis \
  --force-new-deployment
```

### If restart fails: fail-over to Redis replica
```bash
# Update app config to point to replica:
# (In ECS: update task definition env vars REDIS_HOST=redis-replica.internal)

# Verify:
redis-cli -h ${REPLICA_HOST} ping
curl -s http://localhost:8082/actuator/health | jq '.components.redis.status'
```

---

## 4 — After Redis recovers

### 4.1 Verify services self-heal
Spring Boot's Lettuce client reconnects automatically (no restart needed).
```bash
# Should return "UP" within 10–15 seconds after Redis is back:
watch -n 5 'curl -s http://localhost:8082/actuator/health | jq .status'
```

### 4.2 Handle TRANSIENT DLQ messages from fraud-engine
```bash
# Count TRANSIENT messages accumulated during outage:
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  "http://localhost:8084/api/v1/dlq/quarantine?status=OPEN" | jq .totalElements

# Replay them (fraud-engine Redis is healthy now):
curl -X POST \
  -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  -H "Content-Type: application/json" \
  http://localhost:8084/api/v1/dlq/replay \
  -d '{"errorCategoryFilter": "TRANSIENT", "maxMessages": 200, "dryRun": false}'
```

### 4.3 Assess duplicate-transaction risk
During the Redis outage, `tryAcquire()` threw before setting the lock.
The same `idempotencyKey` could have been sent multiple times by retry-happy clients.

```bash
# Check for duplicate idempotency keys in Kafka (scored topic):
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transactions.scored \
  --from-beginning \
  --max-messages 500 \
  --property print.key=true | awk -F'\t' '{print $1}' | sort | uniq -d | head -20
```

If duplicates found → review `scoring_audit_log` and alert records for that window.

---

## 5 — Recovery validation

```bash
# 1. transaction-producer accepting requests:
curl -s -X POST http://localhost:8082/api/v1/transactions \
  -H "Authorization: Bearer ${TEST_JWT}" \
  -H "Content-Type: application/json" \
  -d '{"idempotencyKey":"runbook-test-1","userId":"test","amount":"10.00","currency":"USD","country":"US"}' \
  | jq .status   # → "ACCEPTED"

# 2. Redis health green:
curl -s http://localhost:8082/actuator/health | jq '.components.redis.status'  # → "UP"

# 3. No 503 errors in last 5 min:
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=rate(http_server_requests_seconds_count{status="503"}[5m])' \
  | jq '.data.result[].value[1]'  # → "0"
```

---

## 6 — Post-incident checklist

- [ ] Root cause documented (OOM / crash / network)
- [ ] Redis `maxmemory` tuned if evictions occurred
- [ ] DLQ TRANSIENT messages replayed and verified
- [ ] Duplicate transaction window identified; compliance notified if applicable
- [ ] Consider Redis Sentinel or Redis Cluster for future HA
- [ ] Lettuce pool settings reviewed (`max-active`, `max-wait`)

---

## Escalation

| Condition | Escalate to |
|---|---|
| Redis down > 15 min | Engineering lead |
| Duplicate transactions confirmed | Compliance + Finance |
| Redis memory > 80% for > 24h | Capacity planning |
