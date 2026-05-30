# On-Call Guide & Checklist

## Quick Reference

| What | Where |
|---|---|
| Grafana dashboard | http://localhost:3000 (admin/admin) → Fraud Detection |
| Prometheus | http://localhost:9090 |
| Jaeger (tracing) | http://localhost:16686 |
| Kafka UI | http://localhost:8080 |
| transaction-producer health | http://localhost:8082/actuator/health |
| fraud-engine health | http://localhost:8083/actuator/health |
| alert-service health | http://localhost:8084/actuator/health |
| Quarantine API | `curl -H "X-Internal-Api-Key: $INTERNAL_API_KEY" http://localhost:8084/api/v1/dlq/quarantine` |

### Runbooks index
- [DLQ Spike](runbooks/DLQ_SPIKE.md)
- [Redis Down](runbooks/REDIS_DOWN.md)
- [Schema Mismatch](runbooks/SCHEMA_MISMATCH.md)
- [Consumer Lag](runbooks/CONSUMER_LAG.md)
- [RTO/RPO & Rollback](INCIDENT_RESPONSE.md)

---

## Incident Response Decision Tree

```
PagerDuty fires
      │
      ▼
 ┌────────────────────────────────────┐
 │  1. Acknowledge in PagerDuty      │
 │     (stops escalation timer)       │
 └────────────────────────────────────┘
      │
      ▼
 Is a service returning 503 to customers?
      │
   YES ──→ Check transaction-producer health + Redis → [REDIS_DOWN runbook]
      │
   NO  ──→ Continue below
      │
      ▼
 Is DLQ rate elevated?
      │
   YES ──→ [DLQ_SPIKE runbook] → check error category
      │
   NO  ──→ Continue below
      │
      ▼
 Is consumer lag growing?
      │
   YES ──→ [CONSUMER_LAG runbook] → check DB / GC / traffic
      │
   NO  ──→ Continue below
      │
      ▼
 Are logs showing DeserializationException?
      │
   YES ──→ [SCHEMA_MISMATCH runbook] → rollback producer deployment
      │
   NO  ──→ Check Grafana for anomalies, escalate if unclear
```

---

## Start-of-Shift Checklist (every handoff)

### Health check (2–3 min)
```bash
# All services up?
for port in 8082 8083 8084; do
  echo -n "Port $port: "
  curl -sf http://localhost:${port}/actuator/health | jq -r .status 2>/dev/null || echo "UNREACHABLE"
done

# Consumer lag normal?
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group fraud-engine-consumer-group | grep -v "^$\|GROUP"

# Any OPEN quarantine items?
curl -sf -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  "http://localhost:8084/api/v1/dlq/quarantine?status=OPEN" | jq .totalElements
```

### Grafana check (1 min)
Open the **Fraud Detection** dashboard and verify:
- [ ] DLQ rate panel: < 0.1/s
- [ ] Consumer lag panel: < 500 and stable
- [ ] Error rate panel: < 0.1%
- [ ] Scoring throughput: matches expected TPS
- [ ] Redis memory: < 70%
- [ ] JVM heap (all services): < 80%

### Recent alerts
```bash
# Any alerts fired in last 24h that weren't acknowledged?
# Check #fraud-platform-alerts Slack channel and PagerDuty incident history
```

---

## Active Incident Checklist

### P1 (Critical) — Customer impact or data loss risk

```
Time   Action
00:00  Acknowledge alert (PagerDuty / Slack)
00:02  Post in #fraud-platform-incidents: "Investigating [alert name] — [your name] on it"
00:05  Open relevant runbook, complete triage steps
00:10  Post status update: "Root cause suspected: [X]. Working on fix."
00:15  Apply remediation OR escalate if unclear
00:20  Confirm fix applied, start monitoring recovery metrics
00:30  Post: "Resolved. Monitoring for 10 min." OR escalate to Engineering Lead
00:40  Post final resolution + brief root cause in Slack
00:48  File incident report (see template below)
```

### P2 (Warning) — Degraded but not customer-facing

```
Time   Action
00:00  Read alert in Slack
00:15  Investigate during business hours (no immediate wake-up)
     Check runbook, post findings in #fraud-platform-alerts
04:00  Resolve before end of shift or hand off in shift notes
```

---

## Incident Report Template

```markdown
## Incident Report — [Alert Name] — [Date]

**Severity:** P1 / P2
**Duration:** HH:MM (from first alert to resolution)
**Responder(s):** @handle

### Timeline
- HH:MM — Alert fired
- HH:MM — Investigation started
- HH:MM — Root cause identified: [description]
- HH:MM — Remediation applied: [what was done]
- HH:MM — Service restored

### Root Cause
[1–2 sentences describing why this happened]

### Impact
- Transactions affected: ~N (from consumer lag and DLQ counts)
- Customer-visible: Yes / No
- Data loss: Yes / No
- Compliance notification required: Yes / No

### Resolution
[What was done to fix it]

### Prevention
- [ ] Action item 1 — Owner — Due date
- [ ] Action item 2 — Owner — Due date
```

---

## Shift Notes Template

At end of shift, post in #fraud-platform-oncall:
```
Shift: [Date] [HH:MM]–[HH:MM]
Incidents: [N paged, N resolved, N carried over]
Ongoing issues: [describe or "none"]
System state: [healthy / degraded — what is degraded]
Action items for next on-call: [list or "none"]
```

---

## Useful Commands Reference

### Check Kafka topic health
```bash
# Topic partition details:
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions.raw

# Producer rate (approx, from log-end-offset growth):
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group fraud-engine-consumer-group

# List all consumer groups:
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

### Tail structured logs (JSON)
```bash
# Pretty-print fraud-engine logs with jq:
docker logs ts-fraud-engine -f --tail 50 2>&1 | grep "^{" | jq -r '"\(.["@timestamp"]) [\(.level)] \(.message)"'

# Filter for a specific transactionId:
docker logs ts-fraud-engine --tail 500 2>&1 | grep "transactionId=<TX_ID>"
```

### Check DB state
```bash
psql -h localhost -U tsuser -d transactionshield <<'SQL'
-- Recent scoring activity (last 5 min):
SELECT variant, risk_level, COUNT(*) AS count, AVG(fraud_score) AS avg_score
FROM scoring_audit_log
WHERE evaluated_at > NOW() - INTERVAL '5 minutes'
GROUP BY variant, risk_level ORDER BY count DESC;

-- Recent HIGH/CRITICAL alerts:
SELECT transaction_id, fraud_score, risk_level, created_at
FROM alerts WHERE risk_level IN ('HIGH','CRITICAL')
AND created_at > NOW() - INTERVAL '1 hour'
ORDER BY created_at DESC LIMIT 20;
SQL
```

### Fraud engine rule management
```bash
# List current rule configs:
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  http://localhost:8083/api/v1/rules | jq .

# Force rule refresh from DB:
curl -s -X POST -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  http://localhost:8083/api/v1/rules/refresh | jq .

# Active A/B experiment:
curl -s -H "X-Internal-Api-Key: ${INTERNAL_API_KEY}" \
  http://localhost:8083/api/v1/rules/experiments | jq '.[] | select(.active == true)'
```
