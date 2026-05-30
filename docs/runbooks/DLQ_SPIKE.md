# Runbook: DLQ Spike

**Severity:** P1 (Critical) if rate > 1 msg/s sustained; P2 (Warning) if rate > 0.1 msg/s  
**Services affected:** fraud-engine, alert-service  
**On-call rotation:** Platform / Backend  
**RTO:** 15 min to stop the bleeding; 30 min to full triage  

---

## What is this alert?

The `transactions.dlq` topic is receiving messages at an abnormal rate.
Every DLQ entry represents a transaction that **will not be fraud-scored or alerted** until
it is either replayed or root-cause is fixed.

Prometheus rule that fires this alert:
```
rate(dlq_message_total[5m]) > 1
```

---

## 1 — Immediate triage (≤ 5 min)

### 1.1 Confirm the spike is real
```bash
# Grafana: fraud-detection dashboard → "DLQ Messages / min" panel
# Or direct Prometheus query:
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=rate(dlq_message_total[5m])' | jq .

# Kafka consumer group lag on DLQ topic (is replay consumer keeping up?)
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group dlq-replay-consumer-group
```

### 1.2 Determine error category breakdown
DLQ messages carry `x-error-category` headers (FATAL / NON_RETRYABLE / TRANSIENT).
```bash
# Read DLQ headers from the last 20 messages (kafka-console-consumer):
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transactions.dlq \
  --from-beginning \
  --max-messages 20 \
  --property print.headers=true \
  --property print.timestamp=true

# Or via Kafka UI (localhost:8080) → Topics → transactions.dlq → Messages tab
```

| Category | Meaning | Urgency |
|---|---|---|
| `FATAL` | Deserialization failure, corrupted payload | STOP new publishes, alert immediately |
| `NON_RETRYABLE` | DB constraint violation, invalid business data | Investigate but don't replay |
| `TRANSIENT` | Temporary DB/Kafka unavailability | Safe to replay after infra is healthy |

### 1.3 Check quarantine store
```bash
# alert-service quarantine API:
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  http://localhost:8084/api/v1/dlq/quarantine?status=OPEN | jq '.content | length'
```

---

## 2 — Diagnosis by category

### FATAL — Deserialization exception
Cause: Schema Registry is down OR a backward-incompatible schema was deployed.

```bash
# Check Schema Registry health:
curl -s http://localhost:8081/subjects | jq .

# Check fraud-engine for DeserializationException in logs:
# (JSON log field: "logger_name" contains "DeserializationExceptionHandler")
docker logs ts-fraud-engine 2>&1 | grep -i "deserializ" | tail -20

# Check if a new schema version was recently registered:
curl -s http://localhost:8081/subjects/transactions.raw-value/versions | jq .
```

**→ See also:** [SCHEMA_MISMATCH runbook](./SCHEMA_MISMATCH.md)

### NON_RETRYABLE — DataIntegrityViolationException
Cause: Duplicate event slipped through idempotency (e.g. Redis was down during producer request).
```bash
# Check scoring_audit_log for duplicate transactionIds:
psql -h localhost -U tsuser -d transactionshield \
  -c "SELECT transaction_id, COUNT(*) FROM scoring_audit_log
      GROUP BY transaction_id HAVING COUNT(*) > 1 LIMIT 10;"
```

### TRANSIENT — DB / Redis / Network blip
Cause: fraud-engine failed to write audit log or idempotency check threw.
```bash
# Check fraud-engine DB health:
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  http://localhost:8083/actuator/health | jq '.components.db'

# Check Redis health:
redis-cli -h localhost ping

# Check exponential-backoff retry logs:
docker logs ts-fraud-engine 2>&1 | grep "retry\|backoff" | tail -20
```

---

## 3 — Remediation

### Replay TRANSIENT messages (after infra is healthy)
```bash
# Dry-run first — shows what would be replayed without writing anything:
curl -s -X POST \
  -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  -H "Content-Type: application/json" \
  http://localhost:8084/api/v1/dlq/replay \
  -d '{"maxMessages": 50, "errorCategoryFilter": "TRANSIENT", "dryRun": true}' | jq .

# Live replay (max 100 messages per call, call again if more):
curl -s -X POST \
  -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  -H "Content-Type: application/json" \
  http://localhost:8084/api/v1/dlq/replay \
  -d '{"maxMessages": 100, "errorCategoryFilter": "TRANSIENT", "dryRun": false}' | jq .
```

### Discard NON_RETRYABLE quarantined messages (after review)
```bash
# List OPEN quarantine items:
ITEMS=$(curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  "http://localhost:8084/api/v1/dlq/quarantine?status=OPEN&size=50" | jq -r '.content[].id')

# Discard each (after confirming they are genuinely non-recoverable):
for id in $ITEMS; do
  curl -s -X PATCH \
    -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
    -H "Content-Type: application/json" \
    "http://localhost:8084/api/v1/dlq/quarantine/${id}" \
    -d '{"action": "DISCARD", "reason": "NON_RETRYABLE duplicate — resolved during DLQ spike 2026-XX-XX"}' | jq .status
done
```

---

## 4 — Recovery validation

```bash
# DLQ rate should return to < 0.05/s:
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=rate(dlq_message_total[5m])' | jq .data.result[].value[1]

# Scoring throughput restored:
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=rate(transaction_processed_total[5m])' | jq .

# No new OPEN quarantine items:
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  "http://localhost:8084/api/v1/dlq/quarantine?status=OPEN" | jq .totalElements
```

---

## 5 — Post-incident checklist

- [ ] Root cause identified and documented in incident ticket
- [ ] If FATAL: PR raised to fix schema or re-enable Schema Registry
- [ ] If TRANSIENT: confirm infrastructure is stable for > 30 min before closing
- [ ] CHANGELOG / release notes updated if a deployment caused the spike
- [ ] Alert threshold reviewed — adjust if signal-to-noise ratio was poor
- [ ] Replay count metrics reviewed: were there duplicate alerts generated?

---

## Escalation

| Condition | Escalate to |
|---|---|
| DLQ rate > 10/s for > 5 min | Engineering lead + Product (customer impact) |
| FATAL category > 50% of DLQ | Security / Compliance review (data loss) |
| Replay loop detected (same message re-DLQ'd) | Platform architect |
