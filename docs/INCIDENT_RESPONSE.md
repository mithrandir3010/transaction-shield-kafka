# Incident Response: RTO/RPO & Rollback Procedures

## Service-Level Objectives

| Service | RPO | RTO | Justification |
|---|---|---|---|
| **transaction-producer** | 0 | 5 min | Customer-facing; Redis idempotency ensures no duplicate on retry. Any lost in-flight requests must be retried by clients. |
| **fraud-engine** | 0 | 15 min | Kafka retains `transactions.raw` for 7 days. No events are lost — they queue up. All will be processed once engine recovers. |
| **alert-service** | 0 | 30 min | Kafka retains `transactions.scored` for 7 days. Delayed alerts are acceptable; lost transactions are not. |
| **Redis** | 0 | 5 min | Ephemeral state only (24h TTL keys). No persistence required. Fast restart restores service. |
| **PostgreSQL** | 5 min | 30 min | Durable store: rules, audit log, alerts. Data loss window = since last backup snapshot. |

> **RPO = 0** for Kafka-backed services because the event log is the source of truth.
> Any transaction published to `transactions.raw` WILL be scored, regardless of downstream downtime.

---

## Rollback Procedures

### 1. Service Version Rollback (bad deployment)

**Detect:** Error rate spike immediately after deploy, or DLQ FATAL surge.

```bash
# 1. Identify current and previous image tags:
docker ps --format '{{.Image}}\t{{.Names}}' | grep transaction-shield

# 2. Roll back to previous tag (Docker Compose):
# Edit docker-compose.yml or .env to restore previous IMAGE_TAG
docker compose up -d --no-deps --force-recreate fraud-engine

# ECS Fargate:
aws ecs describe-task-definition --task-definition fraud-engine | jq .taskDefinition.revision
aws ecs update-service \
  --cluster transaction-shield \
  --service fraud-engine \
  --task-definition fraud-engine:<PREVIOUS_REVISION>

# 3. Verify recovery:
watch -n 5 'curl -sf -H "X-Internal-Api-Key: $INTERNAL_API_KEY" \
  http://localhost:8083/actuator/health | jq .status'
```

**Time estimate:** 3–5 min (Docker Compose), 5–10 min (ECS rolling update).

---

### 2. Kafka Consumer Group Offset Reset (re-process events)

Use when: fraud-engine processed events with a buggy rule set and you need to re-score.

⚠️ **This will cause duplicate alert records** unless you clear alerts first or use idempotency.

```bash
# Step 1: Stop the consumer group (scale to 0 or stop service)
docker compose stop fraud-engine

# Step 2: Verify no consumers are active:
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group fraud-engine-consumer-group
# All rows should show: CONSUMER-ID = -

# Step 3a: Reset to specific timestamp (recommended — re-process last N hours):
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group fraud-engine-consumer-group \
  --topic transactions.raw \
  --reset-offsets \
  --to-datetime 2026-05-30T10:00:00.000 \
  --execute

# Step 3b: Reset to beginning (re-process ALL history — expensive):
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group fraud-engine-consumer-group \
  --topic transactions.raw \
  --reset-offsets \
  --to-earliest \
  --execute

# Step 3c: Reset by lag (seek back N messages from current position):
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group fraud-engine-consumer-group \
  --topic transactions.raw \
  --reset-offsets \
  --shift-by -10000 \
  --execute

# Step 4: Restart consumer
docker compose start fraud-engine

# Step 5: Monitor lag drain:
watch -n 10 'kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group fraud-engine-consumer-group'
```

**Time estimate:** 1 min to reset + [lag / throughput] to drain backlog.

---

### 3. Database Migration Rollback

Flyway does not support automatic rollback. Strategies by severity:

#### Scenario A: New migration added a column or table (backward compatible)
No rollback needed — old code ignores new columns.
Simply redeploy old JAR; new DB columns are harmless.

#### Scenario B: Migration changed column type or removed a column
```bash
# 1. Take a DB backup BEFORE the rollback attempt:
pg_dump -h localhost -U tsuser transactionshield > /tmp/ts_backup_$(date +%Y%m%d_%H%M%S).sql

# 2. If new column added, just redeploy old JAR (it will ignore the column)

# 3. If column type changed (e.g. SMALLINT → INTEGER — our V4 migration):
#    The data is safe. Old JARs that wrote SMALLINT-range values still work.
#    No rollback needed; INTEGER is a superset.

# 4. If column removed (NEVER do this in a single deploy — always two-phase):
#    a. Deploy old JAR first (still uses old column)
#    b. Then add the new Flyway repair migration to restore the column
psql -h localhost -U tsuser -d transactionshield \
  -c "ALTER TABLE scoring_audit_log ADD COLUMN old_column VARCHAR(100);"
```

#### Scenario C: Production DB is corrupted — full restore
```bash
# 1. Stop all services:
docker compose stop transaction-producer fraud-engine alert-service

# 2. Restore from backup:
psql -h localhost -U tsuser -c "DROP DATABASE transactionshield;"
psql -h localhost -U tsuser -c "CREATE DATABASE transactionshield;"
psql -h localhost -U tsuser transactionshield < /path/to/backup.sql

# 3. Repair Flyway state (mark migrations as applied):
mvn flyway:repair -pl fraud-engine
mvn flyway:repair -pl alert-service

# 4. Restart services:
docker compose start transaction-producer fraud-engine alert-service
```

**Time estimate:** 15–30 min (depends on backup size and DB restore speed).

---

### 4. Kafka Topic Retention Emergency

If `transactions.raw` fills up storage and retention.ms needs adjustment:

```bash
# Reduce retention (frees disk — messages older than 1h will be deleted):
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic transactions.raw \
  --config retention.ms=3600000   # 1 hour

# Restore to default (7 days) after disk pressure resolves:
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic transactions.raw \
  --config retention.ms=604800000  # 7 days
```

⚠️ This is destructive — messages older than the new retention are DELETED.
Only do this under active disk pressure that threatens service availability.

---

### 5. Rule Engine Emergency Disable

If a fraud rule is causing false positives and blocking legitimate transactions:

```bash
# Disable a specific rule immediately (takes effect within next refresh cycle ≤ 5 min):
curl -X PUT \
  -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  -H "Content-Type: application/json" \
  http://localhost:8083/api/v1/rules/HIGH_AMOUNT/STABLE \
  -d '{"scoreWeight": 0, "enabled": false, "description": "DISABLED — false positive incident 2026-XX-XX", "parameters": {"threshold": "10000"}}'

# Force immediate cache refresh (don't wait for 5-min schedule):
curl -X POST \
  -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  http://localhost:8083/api/v1/rules/refresh

# Verify rule is disabled:
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  http://localhost:8083/api/v1/rules | jq '.[] | select(.ruleCode=="HIGH_AMOUNT")'
```

**Time to take effect:** < 5 seconds after refresh.

---

### 6. A/B Experiment Emergency Deactivation

If an EXPERIMENT rule set is causing degraded scoring quality:

```bash
# Find active experiment ID:
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  http://localhost:8083/api/v1/rules/experiments | jq '.[] | select(.active==true).id'

# Deactivate (returns all traffic to STABLE):
curl -X POST \
  -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  "http://localhost:8083/api/v1/rules/experiments/${EXPERIMENT_ID}/deactivate"
```

**Time to take effect:** < 5 seconds (in-memory switch, no restart).

---

## Recovery Verification Checklist

After any incident, verify all of the following before closing:

```bash
# 1. All services healthy:
for port in 8082 8083 8084; do
  STATUS=$(curl -sf http://localhost:${port}/actuator/health | jq -r .status)
  echo "Port ${port}: ${STATUS}"
done

# 2. No consumer lag:
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group fraud-engine-consumer-group | awk '{print $5}' | grep -v LAG | grep -v "^0$" | wc -l
# → should be 0 (all partitions at 0 lag)

# 3. DLQ rate baseline (< 0.05/s):
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=rate(dlq_message_total[5m])' | jq .data.result

# 4. Scoring rate normal (matches TPS expectation):
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=rate(transaction_processed_total[1m])' | jq .

# 5. No OPEN quarantine items (or count explained):
curl -sf -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  "http://localhost:8084/api/v1/dlq/quarantine?status=OPEN" | jq .totalElements

# 6. End-to-end smoke test:
curl -s -X POST http://localhost:8082/api/v1/transactions \
  -H "Authorization: Bearer ${TEST_JWT}" \
  -H "Content-Type: application/json" \
  -d '{"idempotencyKey":"incident-smoke-test-'$(date +%s)'","userId":"smoke","amount":"50.00","currency":"USD","country":"US"}' \
  | jq .status  # → "ACCEPTED"
```

---

## Escalation Matrix

| Situation | Primary On-Call | Escalate To | SLA |
|---|---|---|---|
| P1 unresolved > 15 min | — | Engineering Lead | Page immediately |
| Customer data confirmed lost | Engineering Lead | Engineering Director + Legal | Within 30 min |
| Compliance-relevant transactions unscored | Engineering Lead | Compliance Officer | Within 1 hour |
| DB backup needed | Platform On-Call | DBA / Infra Lead | Within 15 min |
| Suspected security incident | — | Security Team | Within 15 min |
