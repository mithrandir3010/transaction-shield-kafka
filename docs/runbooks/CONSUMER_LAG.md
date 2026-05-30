# Runbook: Kafka Consumer Lag Increase

**Severity:** P2 (Warning) if lag > 1 000 sustained 10 min; P1 (Critical) if lag > 10 000 or growing unboundedly  
**Services affected:** fraud-engine (transactions.raw), alert-service (transactions.scored)  
**On-call rotation:** Backend / Platform  
**RTO:** 20 min to stop growth; 60 min to drain backlog  
**RPO:** 0 — Kafka retains messages; all will be processed once consumer catches up  

---

## What is this alert?

The Kafka consumer group is not keeping pace with message production.
Lag = messages produced but not yet consumed.

Prometheus rules:
```
# Warning — lag growing steadily
kafka_consumer_lag_sum{group="fraud-engine-consumer-group"} > 1000

# Critical — lag growing without bound (rate > 0 for 10 min)
rate(kafka_consumer_lag_sum{group="fraud-engine-consumer-group"}[10m]) > 0
  and kafka_consumer_lag_sum{group="fraud-engine-consumer-group"} > 500
```

---

## 1 — Immediate triage (≤ 5 min)

```bash
# 1. Current lag per partition:
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group fraud-engine-consumer-group

kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group alert-service-consumer-group

# Output columns: GROUP | TOPIC | PARTITION | CURRENT-OFFSET | LOG-END-OFFSET | LAG | CONSUMER-ID | HOST

# 2. Production rate vs consumption rate (Grafana or Prometheus):
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=rate(kafka_consumer_records_consumed_total{group="fraud-engine-consumer-group"}[1m])' \
  | jq .

# 3. Is the consumer alive?
docker inspect ts-fraud-engine --format='{{.State.Status}}'
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  http://localhost:8083/actuator/health | jq .status

# 4. Check for consumer re-balancing (lots of JoinGroup/SyncGroup in Kafka logs):
docker logs ts-kafka --tail 50 | grep -i "rebalance\|JoinGroup\|Heartbeat"
```

---

## 2 — Diagnosis

### Scenario A: Consumer crashed / OOMKilled
```bash
# Check JVM heap:
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  http://localhost:8083/actuator/metrics/jvm.memory.used | jq .

# Check OOM:
docker inspect ts-fraud-engine --format='{{.State.OOMKilled}}'
# If true: increase container memory limit
```

### Scenario B: Processing slowdown — DB latency
```bash
# Check Postgres query latency:
psql -h localhost -U tsuser -d transactionshield \
  -c "SELECT query, calls, mean_exec_time, max_exec_time
      FROM pg_stat_statements
      ORDER BY mean_exec_time DESC LIMIT 10;"

# Check HikariCP pool saturation:
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  http://localhost:8083/actuator/metrics/hikaricp.connections.active | jq .
# If active ≈ maximum-pool-size (10): pool exhausted, add more connections

# Check for lock waits:
psql -h localhost -U tsuser -d transactionshield \
  -c "SELECT pid, query, state, wait_event_type, wait_event
      FROM pg_stat_activity
      WHERE state != 'idle';"
```

### Scenario C: Rule evaluation slowness (GC pressure)
```bash
# Check GC pause time:
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  http://localhost:8083/actuator/metrics/jvm.gc.pause | jq '.measurements[] | select(.statistic=="MAX")'
# If MAX > 500ms: GC is pausing consumers → tune heap or switch to G1/ZGC

# Check processing time per message:
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=histogram_quantile(0.99, rate(spring_kafka_listener_seconds_bucket[5m]))' | jq .
```

### Scenario D: Traffic spike (normal lag, just high volume)
```bash
# Is this a temporary spike or sustained growth?
curl -s "http://localhost:9090/api/v1/query_range" \
  --data-urlencode 'query=kafka_consumer_lag_sum{group="fraud-engine-consumer-group"}' \
  --data-urlencode 'start=-1h' \
  --data-urlencode 'end=now' \
  --data-urlencode 'step=60' | jq .data.result[].values | length

# Check transaction submission rate:
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=rate(http_server_requests_seconds_count{uri="/api/v1/transactions",method="POST"}[5m])' | jq .
```

### Scenario E: Consumer concurrency too low
```bash
# Current concurrency (matches partition count):
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  http://localhost:8083/actuator/env | jq '.propertySources[] | .properties."app.kafka.consumer-concurrency"'
# Default: 3 (matches 3 partitions of transactions.raw)

# Is concurrency < partition count? Some partitions have no consumer thread:
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group fraud-engine-consumer-group | grep -c "CONSUMER-ID"
# Should be 3 (one per partition)
```

---

## 3 — Remediation

### Scale consumer concurrency (no restart needed if < partition count)
```bash
# Update env var and redeploy (for Docker Compose):
# In .env: KAFKA_CONSUMER_CONCURRENCY=6  (but you also need to increase partitions to 6)

# Scale partitions first (non-destructive — no data loss):
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic transactions.raw \
  --partitions 6

# Then redeploy fraud-engine with KAFKA_CONSUMER_CONCURRENCY=6
```
⚠️ Increasing partitions requires consumer group rebalance. Expect brief lag spike during rebalance.

### Increase `max.poll.records` (higher throughput per poll)
```bash
# Set KAFKA_MAX_POLL_RECORDS=500 in env and redeploy
# Trade-off: higher throughput but larger processing batches → more memory per consumer thread
```
⚠️ Must also tune `max.poll.interval.ms` if processing time > 5 min per batch (unlikely here).

### Increase DB pool to match concurrency
```bash
# If concurrency=6, pool should be >= 6:
# Set DB_POOL_MAX_SIZE=12 in env (headroom for rule refresh + actuator)
```

### Drain backlog with temporary consumer group
```bash
# Add a temporary fraud-engine instance pointing to the same group:
docker run --rm -d \
  --env-file .env \
  --env SPRING_KAFKA_CONSUMER_GROUP_ID=fraud-engine-consumer-group \
  transaction-shield/fraud-engine:latest

# Monitor lag decrease:
watch -n 10 'kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group fraud-engine-consumer-group | grep -v "^$"'
```

---

## 4 — Recovery validation

```bash
# 1. Lag decreasing:
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group fraud-engine-consumer-group
# LAG column should be decreasing every ~30s

# 2. Lag below Warning threshold (< 1000):
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=kafka_consumer_lag_sum{group="fraud-engine-consumer-group"}' \
  | jq .data.result[].value[1]

# 3. Consumer processing rate ≥ production rate:
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=rate(transaction_processed_total[1m])' | jq .

# 4. No DLQ entries caused by timeout/slowness:
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=rate(dlq_message_total[5m])' | jq .
```

---

## 5 — Post-incident checklist

- [ ] Root cause identified (DB slowness / GC / traffic spike / config)
- [ ] `max.poll.records` and concurrency settings reviewed
- [ ] HikariCP pool size aligned with consumer concurrency
- [ ] Kafka retention period verified (default 7 days — was any message dropped?)
- [ ] Capacity plan updated if this was a traffic spike
- [ ] Alert threshold adjusted if false positive (lag normally spikes at deploy time)

---

## Partition / Concurrency Tuning Reference

```
Throughput ≈ (partitions × records_per_poll × processing_speed)

Current defaults:
  partitions       = 3
  concurrency      = 3 (1 thread per partition)
  max.poll.records = 100
  avg processing   ≈ 20ms/record (DB write + rule eval + Redis)

  → theoretical max ≈ 3 × 100 × 50/s = 15 000 TPS
  → real-world (I/O bound) ≈ 1 000–3 000 TPS

Scale-out path:
  1. Increase partitions → 6 (kafka-topics --alter)
  2. Increase KAFKA_CONSUMER_CONCURRENCY → 6
  3. Increase DB_POOL_MAX_SIZE → 14 (6 consumers + 4 headroom + rule refresh)
  4. Add DB read replica for audit log queries if write contention appears
```
