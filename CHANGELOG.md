# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Entries are generated automatically by
[release-please](https://github.com/googleapis/release-please) from
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

---

## [1.0.1](https://github.com/mithrandir3010/transaction-shield-kafka/compare/v1.0.0...v1.0.1) (2026-05-30)


### 🐛 Bug Fixes

* **ci:** image scan advisory-only, add .trivyignore policy file ([f6038f4](https://github.com/mithrandir3010/transaction-shield-kafka/commit/f6038f413b958f4bbfd03f6e5cbf968a567bb8e1))
* **ci:** remove component prefix from release-please config ([7d54384](https://github.com/mithrandir3010/transaction-shield-kafka/commit/7d54384191cddb0c561d9839ec6193ba79f99065))
* **ci:** trivy-action@master, global permissions for checks+security-events ([0e9c78e](https://github.com/mithrandir3010/transaction-shield-kafka/commit/0e9c78e8f69ed29592a7d4a9303127d5d2105c68))
* **tests:** add JWT Bearer token to integration tests after security rollout ([267e6d5](https://github.com/mithrandir3010/transaction-shield-kafka/commit/267e6d50eb96ffb9af3a750de495645c7e701639))


### 📝 Documentation

* **ops:** runbooks, alert thresholds, RTO/RPO, rollback procedures ([14524bc](https://github.com/mithrandir3010/transaction-shield-kafka/commit/14524bcd36112dfd7cc3b7fd15b2364d88da6bcb))

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
