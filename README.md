# 🛡️ Transaction Shield — Real-Time Fraud Detection Engine

<div align="center">

![Java](https://img.shields.io/badge/Java-21-orange?style=for-the-badge&logo=openjdk)
![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.4-brightgreen?style=for-the-badge&logo=springboot)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-7.7-black?style=for-the-badge&logo=apachekafka)
![Apache Avro](https://img.shields.io/badge/Apache_Avro-1.11-purple?style=for-the-badge)
![Redis](https://img.shields.io/badge/Redis-7.4-red?style=for-the-badge&logo=redis)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?style=for-the-badge&logo=postgresql)
![Testcontainers](https://img.shields.io/badge/Testcontainers-1.20-teal?style=for-the-badge)

**A production-grade, event-driven fraud detection system built on Apache Kafka.**  
Processes financial transactions in real-time using a pluggable Rule Engine,  
Redis-powered velocity checks, and distributed idempotency guarantees.

[Architecture](#-system-architecture) · [Features](#-key-technical-features) · [Getting Started](#-getting-started) · [Testing](#-testing-strategy)

</div>

---

## 🎯 Why This Project?

Most fraud detection tutorials stop at "publish to Kafka and log it." This project goes further — it answers the questions that senior engineers actually care about:

- **What happens when the same transaction arrives twice?** → Two-layer idempotency (Redis + DB)
- **How do you detect a user making 10 transactions in 60 seconds?** → Redis ZSET Sliding Window with atomic Lua Script
- **How do you add a new fraud rule without touching existing code?** → Strategy Pattern; one new `@Component` class, zero other changes
- **What happens when Kafka delivery fails?** → Exponential backoff + Dead Letter Queue
- **How do you prevent a schema change from silently breaking downstream consumers?** → Avro + Schema Registry BACKWARD compatibility enforced at CI time

---

## 🏗️ System Architecture

### Data Flow

```mermaid
flowchart LR
    subgraph Ingestion ["🌐 Ingestion Layer"]
        CLIENT(Client) -->|"POST /api/v1/transactions"| PROD["Transaction Producer\n:8082"]
        PROD <-->|"SET NX → 409 if duplicate"| R1[("Redis\nIdempotency")]
    end

    subgraph Detection ["🔍 Fraud Detection"]
        PROD -->|"Publish"| T1[transactions.raw]
        T1 -->|"Consume\n(manual ack)"| ENG["Fraud Engine\n:8083"]
        ENG <-->|"ZADD + ZCARD\nLua Script"| R2[("Redis\nVelocity ZSET")]
        ENG -->|"Evaluate 4 Rules"| RULES{"Rule Engine\n· HighAmount  +50\n· Blacklist   +100\n· Night       +20\n· Velocity    +40/+80"}
        RULES -->|"ScoredTransactionEvent"| T2[transactions.scored]
        ENG -->|"x3 failure"| DLQ[transactions.dlq]
    end

    subgraph Alerting ["🚨 Alert Processing"]
        T2 -->|"Consume"| AS["Alert Service\n:8084"]
        AS -->|"@Retryable x3"| PG[("PostgreSQL\nalerts")]
        AS -->|"HIGH / CRITICAL"| T3[alerts.created]
        AS -->|"GET /api/v1/alerts"| REST["REST API"]
    end
```

### Module Responsibilities

| Module | Port | Responsibility |
|---|---|---|
| `transaction-producer` | 8082 | REST API → Redis idempotency check → `transactions.raw` publish |
| `fraud-engine` | 8083 | Consume → Rule Engine → Score → `transactions.scored` publish |
| `alert-service` | 8084 | Consume → PostgreSQL persist → `alerts.created` (HIGH/CRITICAL only) |
| `common` | — | Avro schemas (`.avsc`), generated Java classes, `AvroMapper`, shared domain records & DTOs |

---

## ✨ Key Technical Features

### 1. 🔁 Distributed Idempotency (Two-Layer Protection)

The system prevents duplicate processing at two independent layers:

**Layer 1 — Redis (Producer):**  
On every `POST /api/v1/transactions`, the producer issues an atomic `SET NX EX` command:

```
SET idempotency:transaction:<idempotencyKey> "PROCESSING" NX EX 86400
```

- Returns `true` → first time seen, proceed to Kafka publish
- Returns `false` → duplicate detected, return `HTTP 409 Conflict`
- **Compensation:** if Kafka publish fails, `DEL` the Redis key so the client can safely retry

**Layer 2 — PostgreSQL (Alert Service):**  
The `alerts` table has a `UNIQUE` constraint on `transaction_id`. If Kafka redelivers the same scored event (at-least-once guarantee), the `INSERT` raises `DataIntegrityViolationException`, which is explicitly **excluded** from `@Retryable` — it's caught silently and the duplicate is discarded.

**Why two layers?**  
Redis is fast but volatile (TTL-based). The DB constraint provides crash-safe, durable deduplication that survives Redis restarts.

---

### 2. ⚡ Velocity Check — Redis Sorted Set Sliding Window

The most sophisticated rule: detecting users who make too many transactions in a short window.

**Why ZSET instead of a simple counter?**

```
Simple INCR + TTL (Fixed Window) — WRONG:
  [00:00–01:00] → 5 transactions, counter resets at 01:00
  Attack: 5 tx at 00:59 + 5 tx at 01:01 = 10 tx detected as 5+5 ✗

ZSET Sliding Window — CORRECT:
  score = epoch_ms, member = transactionId
  At any moment: count entries where score > (now - 60_000ms)
  Attack: 5 tx at 00:59 + 5 tx at 01:01 = 10 tx in window at 01:01 ✓
```

**Why Lua Script instead of Redis Pipeline?**

```
Pipeline (not atomic):
  Thread A: ZADD ...              ──┐
  Thread B: ZADD ...              ──┤ interleaved — race condition
  Thread A: ZREMRANGEBYSCORE ...  ──┘ Thread B's entry visible → wrong count

Lua Script (atomic):
  Redis executes Lua in a single-threaded block.
  ZADD → ZREMRANGEBYSCORE → ZCARD → EXPIRE  (one indivisible unit)
  Also uses EVALSHA: script uploaded once, called by SHA1 hash → 1 RTT
```

The Lua script (loaded at startup via `DefaultRedisScript`):
```lua
redis.call('ZADD',            key, nowMs, transactionId)
redis.call('ZREMRANGEBYSCORE', key, '-inf', nowMs - windowMs)
local count = redis.call('ZCARD', key)
redis.call('EXPIRE', key, ttlSeconds)
return count
```

**Score tiers (configurable via `application.yml`):**

| Window Count | Score Added | Risk Contribution |
|---|---|---|
| ≤ 3 | +0 | Normal behaviour |
| 4 – 5 | +40 | Suspicious velocity |
| > 5 | +80 | High-confidence fraud |

---

### 3. 🧩 Strategy Pattern Rule Engine

Every fraud rule implements a single interface:

```java
public interface FraudRule {
    RuleResult evaluate(TransactionEvent event);
    String getRuleCode();
}
```

Spring auto-collects **all** `@Component` implementations into `List<FraudRule>`:

```java
@Service
public class FraudRuleEngine {
    private final List<FraudRule> rules; // Spring injects ALL FraudRule beans
    // ...
}
```

**Adding a new rule = one new class. Zero other changes.**

```java
@Component
@Order(5)
public class NewDeviceRule implements FraudRule {
    public RuleResult evaluate(TransactionEvent event) { /* ... */ }
    public String getRuleCode() { return "NEW_DEVICE"; }
}
```

This is the **Open/Closed Principle** in production: open for extension, closed for modification.

| Rule | Trigger | Score |
|---|---|---|
| `HighAmountRule` | amount > 10,000 | +50 |
| `BlacklistedCountryRule` | country in [RU, KP, IR, SY, CU] | +100 |
| `NightTransactionRule` | 00:00–05:00 UTC | +20 |
| `VelocityRule` | > 3 tx / min → +40, > 5 tx / min → +80 | +40 / +80 |

Scores are summed and **capped at 100**. Risk level is derived:

```
0–29  → LOW      30–59 → MEDIUM
60–89 → HIGH     90–100 → CRITICAL
```

---

### 4. 🔒 Resilience & Fault Tolerance

**Kafka Dead Letter Queue (fraud-engine + alert-service):**

```
Message consumed
    │
    ▼ (attempt 1 fails)
ExponentialBackOff  →  1s → 2s → 4s  (3 total attempts)
    │
    ▼ (all attempts exhausted)
DeadLetterPublishingRecoverer
    │  preserves original headers: exception class, message, topic, partition, offset
    ▼
transactions.dlq  ←  DlqConsumer monitors and logs for human investigation
```

`DeserializationException` (malformed JSON) bypasses retries entirely — retrying a broken payload three times wastes resources and adds latency.

**Spring @Retryable (alert-service — database layer):**

```java
@Retryable(
    retryFor   = DataAccessException.class,
    noRetryFor = DataIntegrityViolationException.class, // duplicate → skip immediately
    maxAttempts = 3,
    backoff     = @Backoff(delay = 500, multiplier = 2.0, maxDelay = 4_000)
)
@Transactional
public Alert saveAlert(ScoredTransactionEvent event) { ... }
```

Two retry layers, independent of each other:
- **Service-level** `@Retryable`: handles transient DB errors (connection pool exhaustion, deadlocks)
- **Kafka-level** `DefaultErrorHandler`: handles persistent failures after service retries are exhausted

---

### 5. 📐 Schema Governance — Avro + Schema Registry

All Kafka events are serialized with **Apache Avro** and governed by **Confluent Schema Registry**. JSON was the starting point, but it has a critical weakness in multi-service architectures: any field rename, type change, or removal silently corrupts downstream consumers with no compile-time or runtime safety.

**The problem JSON creates:**

```
Producer renames: "userId" → "user_id"
Consumer expects:  "userId"
Result:            userId = null — no exception, silent data loss ✗
```

**How Avro + Schema Registry solves it:**

```
Every message carries a 5-byte prefix: [0x00][schema-id (4 bytes)]
Consumer fetches the schema from Registry by ID and validates every record.
A breaking change is rejected at publish time — before it can reach consumers.
```

**BACKWARD compatibility rule — the only safe default:**

```
BACKWARD means: new schema can read data written by the old schema.
✅ Add an optional field with a default  → safe (old consumers ignore new field)
✅ Remove a field                        → safe (old data still readable)
❌ Add a required field (no default)     → REJECTED by Registry
❌ Change field type string → int        → REJECTED by Registry
```

**Wire format across all topics:**

| Topic | Subject | Schema Class |
|---|---|---|
| `transactions.raw` | `transactions.raw-value` | `TransactionEvent.avsc` |
| `transactions.scored` | `transactions.scored-value` | `ScoredTransactionEvent.avsc` |
| `alerts.created` | `alerts.created-value` | `AlertCreatedEvent.avsc` |

**Domain model vs wire format separation:**

Services work with internal Java records (`TransactionEvent`, `ScoredTransactionEvent`, `AlertCreatedEvent`). Avro-generated classes are used only at the Kafka boundary. `AvroMapper` is the single translation layer:

```java
// Producer: domain record → Avro (at publish time)
kafkaTemplate.send(topic, key, AvroMapper.toAvro(domainEvent));

// Consumer: Avro → domain record (on receipt, before any business logic)
DomainEvent event = AvroMapper.fromAvro(avroRecord);
```

This keeps business logic free of generated code and makes schema evolution transparent to the rest of the codebase.

**Schema compatibility is tested before it reaches CI:**

```java
@Test
void transactionEvent_addRequiredField_isIncompatible() throws Exception {
    MockSchemaRegistryClient registry = new MockSchemaRegistryClient();
    registry.register("transactions.raw-value", TransactionEvent.getClassSchema());
    registry.updateCompatibility("transactions.raw-value", "BACKWARD");

    Schema v2 = SchemaBuilder.record("TransactionEvent")
            .fields().requiredString("newMandatoryField").endRecord();

    assertThat(registry.testCompatibility("transactions.raw-value", v2)).isFalse();
}
```

`SchemaCompatibilityTest` (in `common`) covers six scenarios: add optional field (valid), add required field (rejected), change field type (rejected) — for all three topics.

---

### 6. 🗄️ Database Schema Governance — Flyway

Schema changes are versioned migrations, not ad-hoc SQL:

| File | Content |
|---|---|
| `fraud-engine` `V1__create_base_schema.sql` | `transactions`, `fraud_rules`, `idempotency_log` tables, triggers |
| `fraud-engine` `V2__seed_fraud_rules.sql` | 5 default rule seeds (HIGH_AMOUNT, VELOCITY_CHECK, …) |
| `alert-service` `V1__create_alerts_table.sql` | `alerts` table, indexes, trigger |

Each service maintains its own Flyway history table (`flyway_schema_history_fraud_engine`, `flyway_schema_history_alert_service`) to avoid conflicts on a shared database. `init.sql` is kept only for local bootstrapping — it is not the authoritative schema source.

CI runs `mvn --batch-mode verify` on every push, so a broken migration fails the build before it can reach deployment.

---

## 🛠️ Tech Stack

| Category | Technology | Version | Rationale |
|---|---|---|---|
| Language | Java | 21 | Records for immutable events, pattern matching |
| Framework | Spring Boot | 3.4.1 | Production autoconfiguration, Kafka integration |
| Messaging | Apache Kafka (Confluent) | 7.7.0 | Durable, ordered, replay-capable event stream |
| Serialization | Apache Avro | 1.11.3 | Binary wire format with schema evolution guarantees |
| Schema Registry | Confluent Schema Registry | 7.7.0 | BACKWARD compatibility enforcement, schema versioning |
| Cache / State | Redis | 7.4 | SET NX idempotency, ZSET sliding window |
| Database | PostgreSQL | 16 | Alert persistence, UNIQUE constraint deduplication |
| DB Migrations | Flyway | 10.x | Versioned, per-service schema migrations |
| Resilience | Spring Retry | 2.x | `@Retryable` + `@Recover` for DB fault tolerance |
| Testing | Testcontainers | 1.20 | Real containers, zero mocking |
| Schema Testing | MockSchemaRegistryClient | — | In-memory registry for compatibility tests (no Docker) |
| Async assertions | Awaitility | 4.x | Fluent async test assertions |
| Observability | Spring Actuator | — | `/health`, `/metrics` endpoints |
| Build | Maven (multi-module) | 3.9 | Enforced module boundaries |
| CI | GitHub Actions | — | `mvn verify` on every push; broken migrations fail build |

---

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose
- Java 21+ (for local builds)
- Maven 3.9+ (for local builds)

### Quick Start

**1. Clone the repository**
```bash
git clone https://github.com/your-username/transaction-shield-kafka.git
cd transaction-shield-kafka
```

**2. Start all infrastructure**
```bash
docker compose up -d
```

This starts: Zookeeper, Kafka, Schema Registry, PostgreSQL, Redis, and Kafka UI.  
The `kafka-init` service automatically creates all required topics on first run.

**3. Verify everything is healthy**
```bash
docker compose ps
# All services should be "healthy" or "running"

# Confirm topics were created
docker logs ts-kafka-init
```

**4. Build and run each service**
```bash
# Terminal 1 — Transaction Producer
cd transaction-producer && mvn spring-boot:run

# Terminal 2 — Fraud Engine
cd fraud-engine && mvn spring-boot:run

# Terminal 3 — Alert Service
cd alert-service && mvn spring-boot:run
```

### 🧪 Send Your First Transaction

```bash
curl -X POST http://localhost:8082/api/v1/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "idempotencyKey": "txn-demo-001",
    "userId":         "user-42",
    "amount":         15000.00,
    "currency":       "USD",
    "country":        "RU",
    "deviceFingerprint": "fp-abc-123"
  }'
```

**Expected response (HTTP 202):**
```json
{
  "transactionId": "f3c9b1a2-...",
  "idempotencyKey": "txn-demo-001",
  "status": "ACCEPTED",
  "acceptedAt": "2026-05-14T10:30:00Z"
}
```

**Retry with the same `idempotencyKey` → HTTP 409:**
```json
{
  "status": 409,
  "error": "Conflict",
  "message": "Duplicate transaction detected for idempotency key: txn-demo-001"
}
```

### 📋 Query Alerts

```bash
# All alerts, newest first
curl http://localhost:8084/api/v1/alerts

# Filter by risk level
curl "http://localhost:8084/api/v1/alerts?riskLevel=CRITICAL&page=0&size=10"
```

### 🖥️ Kafka UI

Open [http://localhost:8080](http://localhost:8080) to browse topics, consumer groups, and messages in real time.

### 🔧 Service Ports

| Service | Port |
|---|---|
| Transaction Producer REST API | `8082` |
| Fraud Engine Actuator | `8083/actuator` |
| Alert Service REST API | `8084` |
| Kafka UI | `8080` |
| Schema Registry | `8081` |
| Kafka Broker (external) | `9092` |
| PostgreSQL | `5432` |
| Redis | `6379` |

---

## 🧪 Testing Strategy

### Why Real Containers Instead of Mocks?

Integration tests in this project use **Testcontainers** — real Docker containers for Kafka, Redis, and PostgreSQL. This is a deliberate architectural choice:

```
Mock-based tests verify:   "Does my code call the right methods?"
Container-based tests verify: "Does my system BEHAVE correctly end-to-end?"
```

Specific issues mocks cannot catch that Testcontainers catches:
- Redis `SET NX` semantics differ from a mock's boolean return
- Kafka offset commit ordering with `MANUAL_IMMEDIATE` ack mode
- `DataIntegrityViolationException` from a real PostgreSQL UNIQUE constraint
- Lua script SHA1 caching (`EVALSHA`) and Redis script execution model
- Avro schema ID round-trip: producer registers schema → consumer fetches by ID → deserialization

### Test Architecture

```
AbstractIntegrationTest
├── static KAFKA    (KafkaContainer 7.7.0)  ─┐
├── static POSTGRES (PostgreSQLContainer 16) ├── Startables.deepStart() → parallel startup
├── static REDIS    (GenericContainer 7.4)  ─┘
│
├── @DynamicPropertySource → injects dynamic ports into Spring context
└── @Import(TestKafkaConfig.class)
      ├── rawEventTemplate              (KafkaTemplate<String, avro.TransactionEvent> + KafkaAvroSerializer)
      ├── testKafkaListenerFactory      (KafkaAvroDeserializer, SPECIFIC_AVRO_READER=true)
      └── NewTopic beans                (auto-creates topics via KafkaAdmin)

ScoredEventCollector (@Component in test source)
├── @KafkaListener receives avro.ScoredTransactionEvent
├── AvroMapper.fromAvro() → domain ScoredTransactionEvent
└── BlockingQueue<ScoredTransactionEvent>
    └── poll(Duration timeout) → thread-safe async event capture
```

**Avro in tests — `mock://test-registry`:**

Integration tests use `app.kafka.schema-registry-url: mock://test-registry` (set in `application-test.yml`). This activates Confluent's `MockSchemaRegistryClient` — an in-memory registry that requires no Docker container. All producers and consumers sharing the same mock URL share the same in-memory registry within the JVM, so schema registration and ID lookup work exactly as in production.

```yaml
# application-test.yml (all three services)
app:
  kafka:
    schema-registry-url: mock://test-registry
```

**`SchemaCompatibilityTest` — contract tests without Kafka:**

```
common/src/test/java/.../avro/SchemaCompatibilityTest.java

✅ TransactionEvent   — add optional field (nullable, default=null)  → compatible
❌ TransactionEvent   — add required field (no default)              → REJECTED
❌ TransactionEvent   — change field type string → int               → REJECTED
✅ ScoredTransactionEvent — current schema registers                 → success
✅ ScoredTransactionEvent — add optional field                       → compatible
✅ AlertCreatedEvent  — current schema registers                     → success
❌ AlertCreatedEvent  — add required field                           → REJECTED
```

These tests run in milliseconds (no containers), catch breaking changes at unit-test time, and mirror exactly what Schema Registry enforces in production.

### Test Scenarios

**`FraudEngineIntegrationTest`** — 5 end-to-end scenarios:
```
✅ HighAmount (15,000 USD)          → HIGH_AMOUNT triggered, score=50, MEDIUM
✅ Blacklisted country (RU)         → BLACKLISTED_COUNTRY, score=100, CRITICAL
✅ Night transaction (02:30 UTC)    → SUSPICIOUS_HOUR, score=20, LOW
✅ Multiple rules (15K USD + RU)    → rawScore=150, fraudScore=100 (capped), CRITICAL
✅ Clean transaction (500 USD, US)  → no rules triggered, score=0, LOW
```

**`VelocityRuleIntegrationTest`** — sliding window escalation:
```
Sequential publish-wait loop (guarantees Redis is updated between sends):

tx1 → scored1  count=1  VELOCITY not triggered  ✅
tx2 → scored2  count=2  VELOCITY not triggered  ✅
tx3 → scored3  count=3  VELOCITY not triggered  ✅ (threshold is EXCEEDED, not MET)
tx4 → scored4  count=4  VELOCITY +40 triggered  ✅
tx5 → scored5  count=5  VELOCITY +40 triggered  ✅
tx6 → scored6  count=6  VELOCITY +80 triggered  ✅  riskLevel: HIGH/CRITICAL
```

**`IdempotencyIntegrationTest`** — duplicate rejection:
```
Send same transactionId twice:
  first  → processed → scored event in queue ✅
  second → Redis SET NX returns false → skipped → queue remains empty ✅
  
Send same transactionId three times:
  first  → scored ✅
  second → null (timeout) ✅
  third  → null (timeout) ✅
```

**`AlertServiceIntegrationTest`** — 6 alert service scenarios:
```
✅ HIGH risk event     → alert persisted in DB + AlertCreatedEvent published to Kafka
✅ CRITICAL risk event → alert persisted + AlertCreatedEvent published
✅ LOW risk event      → alert persisted, AlertCreatedEvent NOT published (negative assertion)
✅ MEDIUM risk event   → alert persisted, AlertCreatedEvent NOT published
✅ Duplicate event     → DB UNIQUE constraint → single row (idempotency at storage layer)
✅ Field correctness   → all DB columns match event fields exactly
```

**`DlqRoutingIntegrationTest`** — DLQ routing under persistent failure:
```
@MockBean AlertService → always throws AlertPersistenceException
FastErrorHandlerConfig → FixedBackOff(0ms, 2 retries) — speeds up the test
✅ 3 Kafka attempts exhausted → message routed to transactions.dlq
✅ DLQ record contains Spring Kafka exception headers
✅ Multiple failing messages → each independently routes to DLQ
```

### Running the Tests

```bash
# Schema compatibility tests (no Docker, milliseconds)
mvn test -pl common -Dtest="SchemaCompatibilityTest"

# Fraud engine integration tests
mvn test -pl fraud-engine -Dtest="*IntegrationTest"

# Alert service integration tests
mvn test -pl alert-service -Dtest="*IntegrationTest"

# All modules
mvn verify
```

---

## 📁 Project Structure

```
transaction-shield-kafka/
│
├── common/                              # Shared library — no Spring Boot entrypoint
│   ├── src/main/avro/                   # Avro schema definitions (source of truth)
│   │   ├── TransactionEvent.avsc        # Wire format: transactions.raw-value
│   │   ├── ScoredTransactionEvent.avsc  # Wire format: transactions.scored-value
│   │   └── AlertCreatedEvent.avsc       # Wire format: alerts.created-value
│   │                                    # → avro-maven-plugin generates Java classes into
│   │                                    #   target/generated-sources/avro/com/transactionshield/avro/
│   └── src/main/java/com/transactionshield/common/
│       ├── avro/
│       │   └── AvroMapper.java          # Single translation layer: domain ↔ Avro
│       ├── event/                       # Internal domain records (not exposed on wire)
│       │   ├── TransactionEvent.java
│       │   ├── ScoredTransactionEvent.java
│       │   └── AlertCreatedEvent.java
│       ├── dto/
│       │   ├── TransactionRequest.java  # REST input (Bean Validation)
│       │   └── TransactionResponse.java
│       └── enums/
│           ├── RiskLevel.java           # LOW / MEDIUM / HIGH / CRITICAL
│           └── TransactionStatus.java
│
├── transaction-producer/                # :8082
│   ├── src/main/java/.../producer/
│   │   ├── controller/TransactionController.java
│   │   ├── service/
│   │   │   ├── TransactionProducerService.java  # domain event → AvroMapper → Kafka
│   │   │   └── IdempotencyService.java
│   │   └── config/
│   │       ├── KafkaProducerConfig.java          # KafkaAvroSerializer, acks=all
│   │       └── RedisConfig.java
│   └── src/main/resources/db/migration/        # (no DB in this service)
│
├── fraud-engine/                        # :8083
│   ├── src/main/java/.../engine/
│   │   ├── rule/
│   │   │   ├── FraudRule.java                   # Strategy interface
│   │   │   ├── RuleResult.java
│   │   │   └── impl/
│   │   │       ├── HighAmountRule.java           # @Order(1)
│   │   │       ├── BlacklistedCountryRule.java   # @Order(2)
│   │   │       ├── NightTransactionRule.java     # @Order(3)
│   │   │       └── VelocityRule.java             # @Order(4) — Redis ZSET
│   │   ├── scoring/
│   │   │   ├── FraudRuleEngine.java
│   │   │   └── ScoringResult.java
│   │   ├── service/
│   │   │   ├── FraudEngineService.java
│   │   │   ├── ScoringIdempotencyService.java
│   │   │   └── VelocityCheckService.java
│   │   ├── consumer/
│   │   │   ├── TransactionEventConsumer.java    # receives avro.TransactionEvent → AvroMapper
│   │   │   └── DlqConsumer.java
│   │   └── producer/
│   │       └── ScoredTransactionProducer.java   # AvroMapper → avro.ScoredTransactionEvent
│   ├── src/main/resources/
│   │   ├── db/migration/
│   │   │   ├── V1__create_base_schema.sql       # transactions, fraud_rules, idempotency_log
│   │   │   └── V2__seed_fraud_rules.sql         # 5 default rules
│   │   └── scripts/velocity_check.lua
│
├── alert-service/                       # :8084
│   ├── src/main/java/.../alert/
│   │   ├── entity/Alert.java
│   │   ├── repository/AlertRepository.java
│   │   ├── service/AlertService.java            # @Retryable + @Transactional
│   │   ├── consumer/ScoredTransactionConsumer.java  # receives avro.ScoredTransactionEvent
│   │   ├── producer/AlertEventProducer.java         # AvroMapper → avro.AlertCreatedEvent
│   │   └── controller/AlertController.java
│   └── src/main/resources/db/migration/
│       └── V1__create_alerts_table.sql
│
├── infrastructure/
│   └── postgres/init.sql                # Local bootstrap only — Flyway is authoritative
│
├── .github/workflows/ci.yml             # mvn --batch-mode verify on push/PR
└── docker-compose.yml                   # Kafka, Zookeeper, Schema Registry, PostgreSQL, Redis, Kafka UI
```

---

## 📄 License

This project is licensed under the MIT License.

---

<div align="center">
Built with ☕ Java 21 · 🌿 Spring Boot · 📨 Apache Kafka · ⚡ Redis · 🐘 PostgreSQL
</div>
