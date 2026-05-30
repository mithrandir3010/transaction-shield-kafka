# Alerting Configuration

## Alert Inventory

| Alert | Severity | Threshold | Runbook |
|---|---|---|---|
| `DlqSpikeCritical` | P1 Critical | DLQ rate > 1/s for 2 min | [DLQ_SPIKE](runbooks/DLQ_SPIKE.md) |
| `DlqSpikeWarning` | P2 Warning | DLQ rate > 0.1/s for 5 min | [DLQ_SPIKE](runbooks/DLQ_SPIKE.md) |
| `DlqFatalMessages` | P1 Critical | > 5 FATAL msgs in 10 min | [SCHEMA_MISMATCH](runbooks/SCHEMA_MISMATCH.md) |
| `RedisDown` | P1 Critical | Redis unreachable > 1 min | [REDIS_DOWN](runbooks/REDIS_DOWN.md) |
| `RedisMemoryHigh` | P2 Warning | Memory > 80% for 10 min | [REDIS_DOWN](runbooks/REDIS_DOWN.md) |
| `ConsumerLagCritical` | P1 Critical | Lag > 10 000 for 5 min | [CONSUMER_LAG](runbooks/CONSUMER_LAG.md) |
| `ConsumerLagWarning` | P2 Warning | Lag > 1 000 for 10 min | [CONSUMER_LAG](runbooks/CONSUMER_LAG.md) |
| `ConsumerGroupOffline` | P1 Critical | No active consumers | [CONSUMER_LAG](runbooks/CONSUMER_LAG.md) |
| `SchemaRegistryDown` | P1 Critical | SR unreachable > 2 min | [SCHEMA_MISMATCH](runbooks/SCHEMA_MISMATCH.md) |
| `HighErrorRate` | P1 Critical | HTTP 5xx > 1% for 3 min | — |
| `HighP99Latency` | P2 Warning | p99 > 500ms for 5 min | — |
| `JvmHeapNearMax` | P2 Warning | Heap > 85% for 10 min | — |
| `DbPoolExhausted` | P2 Warning | Pool > 90% for 5 min | — |
| `FraudScoringDropped` | P1 Critical | 0 scorings for 3 min | — |
| `HighCriticalAlertRate` | P2 Warning | > 10 CRITICAL/s for 2 min | — |

---

## Routing: PagerDuty (P1) + Slack (P2)

### Alertmanager configuration

```yaml
# alertmanager.yml
global:
  resolve_timeout: 5m

route:
  group_by: [alertname, service]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  receiver: slack-warnings

  routes:
    # P1 Critical → PagerDuty (wake up on-call)
    - matchers:
        - severity="critical"
      receiver: pagerduty-critical
      continue: true      # also send to Slack for visibility

    # Fraud-ops team gets their own Slack channel for quality alerts
    - matchers:
        - team="fraud-ops"
      receiver: slack-fraud-ops

receivers:
  - name: pagerduty-critical
    pagerduty_configs:
      - service_key: ${PAGERDUTY_SERVICE_KEY}
        description: '{{ template "pagerduty.default.description" . }}'
        details:
          runbook: '{{ (index .Alerts 0).Annotations.runbook }}'
          description: '{{ (index .Alerts 0).Annotations.description }}'

  - name: slack-warnings
    slack_configs:
      - api_url: ${SLACK_WEBHOOK_URL}
        channel: '#fraud-platform-alerts'
        title: '[{{ .Status | toUpper }}{{ if eq .Status "firing" }} 🔴{{ else }} ✅{{ end }}] {{ .CommonAnnotations.summary }}'
        text: |
          *Description:* {{ .CommonAnnotations.description }}
          {{ if .CommonAnnotations.runbook }}*Runbook:* <{{ .CommonAnnotations.runbook }}|📖 Open>{{ end }}
          *Alerts:* {{ len .Alerts }}
        send_resolved: true

  - name: slack-fraud-ops
    slack_configs:
      - api_url: ${SLACK_FRAUD_OPS_WEBHOOK_URL}
        channel: '#fraud-ops-alerts'
        title: '{{ .CommonAnnotations.summary }}'
        text: '{{ .CommonAnnotations.description }}'
        send_resolved: true

inhibit_rules:
  # If Redis is down, suppress RedisMemoryHigh (root cause already paging)
  - source_matchers: [alertname="RedisDown"]
    target_matchers: [alertname="RedisMemoryHigh"]
    equal: [instance]

  # If SchemaRegistry is down, suppress DlqFatalMessages (derived symptom)
  - source_matchers: [alertname="SchemaRegistryDown"]
    target_matchers: [alertname="DlqFatalMessages"]
```

### Environment variables required
```bash
PAGERDUTY_SERVICE_KEY=<from PagerDuty integration settings>
SLACK_WEBHOOK_URL=<from Slack app → Incoming Webhooks>
SLACK_FRAUD_OPS_WEBHOOK_URL=<from Slack app → fraud-ops channel>
```

---

## Prometheus scrape configuration

Add to `prometheus.yml`:
```yaml
scrape_configs:
  - job_name: transaction-producer
    static_configs:
      - targets: ['host.docker.internal:8082']
    metrics_path: /actuator/prometheus

  - job_name: fraud-engine
    static_configs:
      - targets: ['host.docker.internal:8083']
    metrics_path: /actuator/prometheus

  - job_name: alert-service
    static_configs:
      - targets: ['host.docker.internal:8084']
    metrics_path: /actuator/prometheus

  - job_name: redis
    static_configs:
      - targets: ['redis-exporter:9121']    # redis_exporter sidecar

  - job_name: kafka
    static_configs:
      - targets: ['kafka-exporter:9308']    # kafka_exporter sidecar

  - job_name: schema-registry
    static_configs:
      - targets: ['host.docker.internal:8081']
    metrics_path: /metrics

rule_files:
  - /etc/prometheus/alerts/transaction-shield.yml
```

---

## SLO Summary

| Service | Availability SLO | p99 Latency SLO | Error Budget (30d) |
|---|---|---|---|
| transaction-producer POST | 99.9% | < 200ms | 43 min/month |
| fraud scoring throughput | 99.5% | < 500ms | 3.6 hr/month |
| alert creation | 99.0% | < 1s | 7.2 hr/month |

Error budget is consumed when:
- `HighErrorRate` alert is firing (5xx rate > 1%)
- `ConsumerLagCritical` is firing (scoring delayed > lag/production-rate)
- `FraudScoringDropped` is firing (0 scorings per period)
