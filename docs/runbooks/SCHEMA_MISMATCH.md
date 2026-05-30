# Runbook: Avro Schema Mismatch

**Severity:** P1 — all messages from affected topic fail to deserialize → DLQ storm  
**Services affected:** fraud-engine (transactions.raw consumer), alert-service (transactions.scored consumer)  
**On-call rotation:** Backend / Platform  
**RTO:** 10 min (rollback deployment) | 30 min (schema fix and re-deploy)  
**RPO:** 0 — Kafka retains all messages; they can be replayed after schema is fixed  

---

## What is this alert?

A `DeserializationException` is thrown when a Kafka consumer attempts to deserialize an
Avro message whose schema is incompatible with the registered schema.

**Most common causes:**
1. A producer was deployed with a new schema that is **not BACKWARD compatible** with the
   last registered version (new required field without default, renamed field, type change)
2. Schema Registry is **unreachable** — consumer can't fetch schema → falls back to cached
   schema → mismatch if producer used a newer schema
3. A wrong Avro `.avsc` file was committed and CI contract tests were bypassed

Prometheus rule:
```
increase(dlq_message_total{original_topic="transactions.raw"}[5m]) > 5
```
Combined with logs containing `DeserializationException`.

---

## 1 — Immediate triage (≤ 5 min)

```bash
# 1. Check for DeserializationException in fraud-engine logs:
docker logs ts-fraud-engine 2>&1 | grep -i "DeserializationException\|schema\|avro" | tail -30

# 2. Check DLQ for FATAL error category (DeserializationException → FATAL):
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transactions.dlq \
  --max-messages 10 \
  --property print.headers=true \
  --timeout-ms 5000 | grep "x-error-category"
# Expected header: x-error-category:FATAL

# 3. Check Schema Registry status:
curl -s http://localhost:8081/subjects | jq .
curl -s http://localhost:8081/subjects/transactions.raw-value/versions | jq .

# 4. Fetch the currently registered schema:
curl -s "http://localhost:8081/subjects/transactions.raw-value/versions/latest" | jq .schema | python3 -m json.tool
```

---

## 2 — Diagnosis

### Scenario A: Backward-incompatible schema deployed
```bash
# Compare latest registered schema vs current code:
# In repo: cat common/src/main/avro/TransactionEvent.avsc

# Get registered version N and N-1 and diff:
LATEST=$(curl -s http://localhost:8081/subjects/transactions.raw-value/versions/latest | jq .version)
PREV=$((LATEST - 1))

curl -s "http://localhost:8081/subjects/transactions.raw-value/versions/${LATEST}" | jq -r .schema > /tmp/schema_latest.json
curl -s "http://localhost:8081/subjects/transactions.raw-value/versions/${PREV}" | jq -r .schema > /tmp/schema_prev.json
diff /tmp/schema_prev.json /tmp/schema_latest.json

# Check compatibility mode:
curl -s "http://localhost:8081/config/transactions.raw-value" | jq .
# Should be: {"compatibilityLevel":"BACKWARD"}
```

Common breaking changes:
- New field **without** a `default` → breaks old consumers reading new messages  
- Field type change (e.g. `int` → `long`)  
- Field rename (Avro doesn't have rename, treated as delete + add)  
- Enum value addition without `default` on the enum field  

### Scenario B: Schema Registry unreachable
```bash
# From fraud-engine container:
docker exec ts-fraud-engine sh -c "curl -s http://schema-registry:8081/subjects"
# Timeout or error = Schema Registry unreachable from container network

# Check Schema Registry container:
docker inspect ts-schema-registry --format='{{.State.Status}}'
docker logs ts-schema-registry --tail 30
```

---

## 3 — Remediation

### Fast path: roll back the producer deployment
This stops new incompatible messages from being produced.
Existing incompatible messages in `transactions.raw` will still be in DLQ —
they cannot be replayed until the schema issue is resolved.

```bash
# Docker Compose: bring back previous image tag
docker compose up -d --no-deps transaction-producer
# (update docker-compose.yml to previous image tag first)

# ECS:
aws ecs update-service \
  --cluster transaction-shield \
  --service transaction-producer \
  --task-definition transaction-producer:<PREVIOUS_REVISION>

# Verify: no new DeserializationExceptions in last 2 min:
docker logs ts-fraud-engine --since 2m 2>&1 | grep -c "DeserializationException"
```

### Fix the schema (if rollback is not feasible)
1. Add a `default` to any new fields:
   ```json
   {"name": "newField", "type": ["null", "string"], "default": null}
   ```
2. Update `common/src/main/avro/TransactionEvent.avsc`
3. Run contract tests locally:
   ```bash
   mvn test -pl common -Dtest=SchemaCompatibilityTest
   ```
4. Re-deploy transaction-producer **before** deploying fraud-engine/alert-service

### Handle Schema Registry outage
```bash
# Restart Schema Registry:
docker compose restart schema-registry

# Verify:
curl -s http://localhost:8081/subjects | jq . | head -5

# Both producers and consumers will reconnect automatically within ~30s
```

### Delete the incompatible schema version (emergency only)
⚠️ Only do this if the version was registered by mistake and no messages have been produced with it yet.
```bash
# Soft delete (marks as DELETED, still recoverable):
curl -X DELETE \
  "http://localhost:8081/subjects/transactions.raw-value/versions/${LATEST}"

# Verify:
curl -s "http://localhost:8081/subjects/transactions.raw-value/versions" | jq .
```

---

## 4 — Replay FATAL DLQ messages after fix

FATAL messages (DeserializationException) are **NOT replayed automatically** — they are
quarantined. After the schema is fixed and consumers are updated:

```bash
# Check what's in quarantine:
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  "http://localhost:8084/api/v1/dlq/quarantine?status=OPEN&size=100" | jq .totalElements

# FATAL messages cannot be replayed via the replay API — the payload is corrupted.
# They must be either:
# a) Resolved manually (original transaction re-submitted by client)
# b) Discarded after compliance review

# Discard after review:
curl -X PATCH \
  -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  -H "Content-Type: application/json" \
  "http://localhost:8084/api/v1/dlq/quarantine/${ID}" \
  -d '{"action":"DISCARD","reason":"Schema mismatch incident 2026-XX-XX — message unrecoverable"}'
```

---

## 5 — Recovery validation

```bash
# 1. No new DeserializationExceptions:
docker logs ts-fraud-engine --since 5m 2>&1 | grep -c "DeserializationException"  # → 0

# 2. DLQ rate back to baseline:
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=rate(dlq_message_total[5m])' | jq .data.result[].value[1]

# 3. Scoring throughput normal:
curl -s "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=rate(transaction_processed_total[1m])' | jq .

# 4. Schema compatibility confirmed:
curl -s "http://localhost:8081/compatibility/subjects/transactions.raw-value/versions/latest" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schema\": \"$(cat common/src/main/avro/TransactionEvent.avsc | jq -c . | jq -Rs .)\"}" | jq .
```

---

## 6 — Prevention

- CI contract tests (`SchemaCompatibilityTest`) catch incompatible changes before merge
- Schema Registry has `BACKWARD` compatibility mode enforced — never change to `NONE`
- Breaking changes require a **two-phase deploy**: old consumer first, then new producer
- Never bypass CI with `git push --force` or `--no-verify`

---

## Post-incident checklist

- [ ] Breaking change or Schema Registry failure — which was it?
- [ ] Incompatible schema version deleted from Schema Registry (if applicable)
- [ ] Affected transactions identified; clients asked to retry or transactions replayed
- [ ] Compliance notified if transactions in a regulatory window were lost
- [ ] `SchemaCompatibilityTest` updated to cover the new field/change
- [ ] Post-mortem scheduled within 48h
