# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Entries are generated automatically by
[release-please](https://github.com/googleapis/release-please) from
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

---

## [1.0.0] — 2026-05-30

### ✨ Features

- **Fraud Engine** — DB/config-backed rule system with score weights, A/B experiment routing,
  and scoring audit log (`feat(rule-engine)`)
- **DLQ taxonomy** — error classification (FATAL/NON_RETRYABLE/TRANSIENT), replay service,
  quarantine store, and operator REST API (`feat(dlq)`)
- **Observability** — Micrometer/Prometheus metrics, OpenTelemetry tracing (Jaeger),
  structured JSON logging via Logstash encoder (`feat(observability)`)
- **Avro/Schema Registry** — Confluent Schema Registry integration, BACKWARD compatibility
  CI checks, contract tests (`feat(avro)`)
- **Security** — JWT (HS256) resource server on transaction-producer, internal API key
  auth on fraud-engine/alert-service, PII masking (userId), secret env-var externalisation,
  Logback `MaskingJsonGeneratorDecorator` safety net (`feat(security)`)
- **Performance** — K6 load tests (baseline/stress/spike/soak) with SLO thresholds,
  configurable Kafka consumer concurrency and `max.poll.records`, HikariCP tuning,
  Kafka producer batching + snappy compression (`feat(perf)`)

### 🐛 Bug Fixes

- Widen `SMALLINT` columns to `INTEGER` to fix Hibernate schema validation (`fix(fraud-engine)`)
- Skip Testcontainer integration tests gracefully when Docker is unavailable (`fix(tests)`)
- Correct `score_weight` seed values in V2 migration (`fix(rule-engine)`)
- `DataAccessException` (Redis down) now returns HTTP 503 instead of 500 (`fix(resilience)`)

### 🧪 Tests

- Chaos resilience tests: `KafkaDownChaosTest`, `RedisDownChaosTest`,
  `PostgresDownResilienceTest` — tagged `@Tag("chaos")`, excluded from default build

---
<!-- release-please-start-version -->
<!-- release-please-end-version -->
