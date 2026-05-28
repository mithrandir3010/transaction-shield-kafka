package com.transactionshield.common.avro;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Schema governance tests — verifies BACKWARD compatibility policy per topic subject.
 *
 * BACKWARD compatibility rule: a new schema version must be able to read data
 * written by any previous version.
 *   ✓ Add optional field (null union, null default) — old data has no value → default fills it
 *   ✗ Add required field (no default)               — old data has no value → unreadable
 *   ✗ Change field type (string → int)               — old data has string → unreadable
 *
 * Uses MockSchemaRegistryClient — no Docker required, fully in-memory.
 * These tests run as part of mvn verify and FAIL the build on breaking schema changes.
 *
 * Subject naming follows TopicNameStrategy: <topic>-value (e.g. transactions.raw-value).
 */
@DisplayName("Schema Governance — BACKWARD Compatibility Tests")
class SchemaCompatibilityTest {

    private static final String RAW_SUBJECT    = "transactions.raw-value";
    private static final String SCORED_SUBJECT = "transactions.scored-value";
    private static final String ALERT_SUBJECT  = "alerts.created-value";

    private SchemaRegistryClient registry;

    @BeforeEach
    void setUp() {
        registry = new MockSchemaRegistryClient();
    }

    // ── TransactionEvent ────────────────────────────────────────────────────────

    @Test
    @DisplayName("TransactionEvent — add optional field → BACKWARD compatible")
    void transactionEvent_addOptionalField_isCompatible() throws Exception {
        Schema v1 = com.transactionshield.avro.TransactionEvent.getClassSchema();
        registry.register(RAW_SUBJECT, new AvroSchema(v1));
        registry.updateCompatibility(RAW_SUBJECT, "BACKWARD");

        // V2: adds nullable channelSource (optional, null default) — backward compatible
        Schema v2 = new Schema.Parser().parse("""
                {
                  "type": "record", "name": "TransactionEvent",
                  "namespace": "com.transactionshield.avro",
                  "fields": [
                    {"name": "transactionId",     "type": "string"},
                    {"name": "idempotencyKey",     "type": "string"},
                    {"name": "userId",             "type": "string"},
                    {"name": "amount",             "type": "string"},
                    {"name": "currency",           "type": "string"},
                    {"name": "country",            "type": "string"},
                    {"name": "deviceFingerprint",  "type": ["null","string"], "default": null},
                    {"name": "timestampMillis",    "type": "long"},
                    {"name": "channelSource",      "type": ["null","string"], "default": null}
                  ]
                }""");

        assertThat(registry.testCompatibility(RAW_SUBJECT, new AvroSchema(v2)))
                .as("Adding an optional field must be BACKWARD compatible")
                .isTrue();
    }

    @Test
    @DisplayName("TransactionEvent — add required field (no default) → BACKWARD incompatible")
    void transactionEvent_addRequiredField_isIncompatible() throws Exception {
        Schema v1 = com.transactionshield.avro.TransactionEvent.getClassSchema();
        registry.register(RAW_SUBJECT, new AvroSchema(v1));
        registry.updateCompatibility(RAW_SUBJECT, "BACKWARD");

        // V2: adds required processingRegion (no default) — breaking: old records lack this field
        Schema v2 = new Schema.Parser().parse("""
                {
                  "type": "record", "name": "TransactionEvent",
                  "namespace": "com.transactionshield.avro",
                  "fields": [
                    {"name": "transactionId",     "type": "string"},
                    {"name": "idempotencyKey",     "type": "string"},
                    {"name": "userId",             "type": "string"},
                    {"name": "amount",             "type": "string"},
                    {"name": "currency",           "type": "string"},
                    {"name": "country",            "type": "string"},
                    {"name": "deviceFingerprint",  "type": ["null","string"], "default": null},
                    {"name": "timestampMillis",    "type": "long"},
                    {"name": "processingRegion",   "type": "string"}
                  ]
                }""");

        assertThat(registry.testCompatibility(RAW_SUBJECT, new AvroSchema(v2)))
                .as("Adding a required field (no default) must NOT be BACKWARD compatible")
                .isFalse();
    }

    @Test
    @DisplayName("TransactionEvent — change field type string→int → BACKWARD incompatible")
    void transactionEvent_changeFieldType_isIncompatible() throws Exception {
        Schema v1 = com.transactionshield.avro.TransactionEvent.getClassSchema();
        registry.register(RAW_SUBJECT, new AvroSchema(v1));
        registry.updateCompatibility(RAW_SUBJECT, "BACKWARD");

        // V2: changes amount from string to int — breaks old string-encoded values
        Schema v2 = new Schema.Parser().parse("""
                {
                  "type": "record", "name": "TransactionEvent",
                  "namespace": "com.transactionshield.avro",
                  "fields": [
                    {"name": "transactionId",     "type": "string"},
                    {"name": "idempotencyKey",     "type": "string"},
                    {"name": "userId",             "type": "string"},
                    {"name": "amount",             "type": "int"},
                    {"name": "currency",           "type": "string"},
                    {"name": "country",            "type": "string"},
                    {"name": "deviceFingerprint",  "type": ["null","string"], "default": null},
                    {"name": "timestampMillis",    "type": "long"}
                  ]
                }""");

        assertThat(registry.testCompatibility(RAW_SUBJECT, new AvroSchema(v2)))
                .as("Changing a field type must NOT be BACKWARD compatible")
                .isFalse();
    }

    // ── ScoredTransactionEvent ──────────────────────────────────────────────────

    @Test
    @DisplayName("ScoredTransactionEvent — current schema registers and is self-compatible")
    void scoredTransactionEvent_currentSchema_registersSuccessfully() throws Exception {
        Schema v1 = com.transactionshield.avro.ScoredTransactionEvent.getClassSchema();
        int id = registry.register(SCORED_SUBJECT, new AvroSchema(v1));
        registry.updateCompatibility(SCORED_SUBJECT, "BACKWARD");

        assertThat(id).isGreaterThan(0);
        // Re-registering same schema should be idempotent (returns same id)
        assertThat(registry.register(SCORED_SUBJECT, new AvroSchema(v1))).isEqualTo(id);
    }

    @Test
    @DisplayName("ScoredTransactionEvent — add optional metadata field → BACKWARD compatible")
    void scoredTransactionEvent_addOptionalField_isCompatible() throws Exception {
        Schema v1 = com.transactionshield.avro.ScoredTransactionEvent.getClassSchema();
        registry.register(SCORED_SUBJECT, new AvroSchema(v1));
        registry.updateCompatibility(SCORED_SUBJECT, "BACKWARD");

        // Build V2 by adding a nullable engineVersion field
        Schema v2 = addNullableField(v1, "engineVersion");

        assertThat(registry.testCompatibility(SCORED_SUBJECT, new AvroSchema(v2)))
                .as("Adding optional engineVersion must be BACKWARD compatible")
                .isTrue();
    }

    // ── AlertCreatedEvent ───────────────────────────────────────────────────────

    @Test
    @DisplayName("AlertCreatedEvent — current schema registers successfully")
    void alertCreatedEvent_currentSchema_registersSuccessfully() throws Exception {
        Schema v1 = com.transactionshield.avro.AlertCreatedEvent.getClassSchema();
        int id = registry.register(ALERT_SUBJECT, new AvroSchema(v1));
        registry.updateCompatibility(ALERT_SUBJECT, "BACKWARD");

        assertThat(id).isGreaterThan(0);
    }

    @Test
    @DisplayName("AlertCreatedEvent — add required field (no default) → BACKWARD incompatible")
    void alertCreatedEvent_addRequiredField_isIncompatible() throws Exception {
        Schema v1 = com.transactionshield.avro.AlertCreatedEvent.getClassSchema();
        registry.register(ALERT_SUBJECT, new AvroSchema(v1));
        registry.updateCompatibility(ALERT_SUBJECT, "BACKWARD");

        // V2 adds a required notificationChannel — old alerts lack this value
        Schema v2 = new Schema.Parser().parse("""
                {
                  "type": "record", "name": "AlertCreatedEvent",
                  "namespace": "com.transactionshield.avro",
                  "fields": [
                    {"name": "alertId",            "type": "string"},
                    {"name": "transactionId",      "type": "string"},
                    {"name": "userId",             "type": "string"},
                    {"name": "fraudScore",         "type": "int"},
                    {"name": "riskLevel",          "type": "string"},
                    {"name": "triggeredRules",     "type": {"type":"array","items":"string"}, "default": []},
                    {"name": "createdAtMillis",    "type": "long"},
                    {"name": "notificationChannel","type": "string"}
                  ]
                }""");

        assertThat(registry.testCompatibility(ALERT_SUBJECT, new AvroSchema(v2)))
                .as("Adding a required field must NOT be BACKWARD compatible")
                .isFalse();
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    /**
     * Returns a new Schema that is identical to the given schema but with an extra
     * nullable String field (with null default) appended. Used to generate a
     * BACKWARD-compatible V2 without duplicating large schema strings.
     */
    private static Schema addNullableField(Schema base, String fieldName) {
        String nullable = "[\"null\",\"string\"]";
        String originalJson = base.toString();
        String insertion = ",{\"name\":\"" + fieldName + "\",\"type\":" + nullable + ",\"default\":null}";
        // Insert before the final "]}" which closes the fields array and the record object.
        // Using lastIndexOf avoids collisions with embedded "]}" patterns (e.g. "default":[]).
        int lastIdx = originalJson.lastIndexOf("]}");
        String v2Json = originalJson.substring(0, lastIdx) + insertion + originalJson.substring(lastIdx);
        return new Schema.Parser().parse(v2Json);
    }
}
