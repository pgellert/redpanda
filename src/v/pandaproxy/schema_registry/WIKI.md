# Redpanda Schema Registry - Technical Overview

> **Quick Start**: See [Quick Reference](#quick-reference) for a compact overview of architecture, operations, and debugging.

## Table of Contents
1. [Quick Reference](#quick-reference) ⚡ *Start here for fast lookup*
2. [What is Schema Registry?](#what-is-schema-registry)
3. [Why Do We Need It?](#why-do-we-need-it)
4. [Key Concepts & Terminology](#key-concepts--terminology)
5. [Architecture Overview](#architecture-overview)
6. [How It Works: Internal Design](#how-it-works-internal-design)
7. [Schema Processing: Sanitization, Canonicalization, and Normalization](#schema-processing-sanitization-canonicalization-and-normalization)
8. [Schema Formats](#schema-formats)
9. [Compatibility Levels](#compatibility-levels)
10. [Common Operations](#common-operations)
11. [Advanced Topics](#advanced-topics)

---

## Quick Reference

> **Purpose**: Compact reference for quick lookup, onboarding, and AI coding assistants. Focuses on architectural invariants.

### Core Architecture at a Glance

**Storage Model**
- Backend: Single-partition `_schemas` topic (Kafka-style, compacted)
- Frontend: In-memory full replica on every broker
- Propagation: Write → topic → consume back → apply to memory
- HA: Active/Active - any broker serves reads/writes, no SPOF

**Consistency Model**
- Optimistic concurrency, first-writer-wins via offset-as-sequence
- Retries on collision when multiple writers race
- Schema IDs: Globally unique, monotonically increasing, content-based deduplication

**Sharding**
- Per-CPU-core stores with full schema copies
- Requests hash-routed by subject/schema_id to specific shard
- Write coordination via shard 0 ("reader shard")

### Key Operations Flow

**Schema Registration**
```
Client POST → Canonicalize (sanitize + normalize + resolve refs)
→ Check compatibility → Deduplicate (reuse ID if exists)
→ Allocate ID locally → Optimistic write:
    read_sync() → produce to topic → consume own write
    → apply to memory → check offset match
    → if mismatch: retry | if match: return ID
```

**Schema Retrieval**
- **Fast path**: Serve from memory (fallback to sync if missing)
- **Slow path**: Explicit sync for list operations (subjects, versions)

**Write Synchronization**
- Offset-as-sequence: Expected offset = latest_offset + 1
- Actual offset returned by topic write
- Match = success | Mismatch = collision, retry

### Processing Pipeline

| Stage | Purpose | Output |
|-------|---------|--------|
| **Sanitization** | Validate, parse, compact | Valid schema |
| **Canonicalization** | Sanitize + optional normalize + resolve refs | Stored schema |
| **Normalization** (optional) | Deterministic ordering for deduplication | Consistent byte representation |

**Config**: `schema_registry_always_normalize` (default: false)

### Schema Identity

```
Subject: users-value
  Version 1 → Schema ID 42 (initial)
  Version 2 → Schema ID 58 (evolved)
  Version 3 → Schema ID 42 (same schema as v1)

Subject: orders-value
  Version 1 → Schema ID 42 (same schema = same ID)
```

### Compatibility Modes Quick Table

| Mode | Ensures | Use Case |
|------|---------|----------|
| **BACKWARD** | New reads old data | Upgrade consumers first |
| **FORWARD** | Old reads new data | Upgrade producers first |
| **FULL** | Both directions | Any upgrade order |
| **_TRANSITIVE** | All versions (not just previous) | Strictest checking |
| **NONE** | No checking | Use with caution |

### References

- **Internal** (registry-managed): ✅ Avro, Protobuf, JSON Schema
- **External** (public URIs): ❌ JSON Schema not supported | Limited Avro/Protobuf

### Common Issues & Debugging

**Slow Writes (20-200ms)**
- Normal: Every write goes through topic + consume-own-write
- Check: Logs for "write_subject_version", "sequenced_write" for retries

**404 on Recent Schema**
- Cause: Node's memory behind, will self-heal on next sync
- Action: Usually transient, retry or check metrics

**Write Contention**
- Symptom: Frequent offset mismatches in logs
- Solution: Route writes to same node (load balancer stickiness)

**Memory Growth**
- Check: `vectorized_schema_registry_cache_schema_count` metric
- Action: Review schema cleanup policy

### REST API Cheat Sheet

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/subjects` | GET | List subjects |
| `/subjects/{subject}/versions` | POST | Register schema |
| `/subjects/{subject}/versions/{version}` | GET | Get schema |
| `/subjects/{subject}` | DELETE | Soft delete |
| `/subjects/{subject}?permanent=true` | DELETE | Hard delete (after soft) |
| `/schemas/ids/{id}` | GET | Get by ID |
| `/config` | GET/PUT | Global compatibility |
| `/compatibility/subjects/{subject}/versions/{version}` | POST | Test compatibility |

### Integration Points

- **Auth**: HTTP Basic, mTLS, SASL/SCRAM, OIDC (Enterprise)
- **Authz** (Enterprise): Per-subject ACLs, audit logging
- **Schema ID Validation** (Enterprise): Server-side validation in topic
- **Iceberg**: Schema evolution for Iceberg tables
- **Data Transforms**: Schema-aware Wasm transforms

### Future: Contexts (Planned)
- Schema IDs unique per context (namespace), not globally
- GUID for cross-context identity
- Multi-tenancy support

---

## What is Schema Registry?

A centralized service that manages schemas for streaming data, providing:
- Schema registration and versioning
- Compatibility validation during evolution
- Schema retrieval by ID or subject
- Governance and discovery

**Key Value**: Ensures producers and consumers agree on data structure, enables safe schema evolution, and reduces payload size (schema ID instead of full schema in each message).

**Confluent Compatibility**:
- ✅ REST API compatible (Confluent clients work with Redpanda)
- ⚠️ Internal storage format differs (cannot migrate storage directly)

---

## Key Concepts & Terminology

> **See [Quick Reference](#quick-reference)** for architecture and operations overview.

### Core Types

**Subject**: Name under which schemas are registered (e.g., `user-events-value`, `user-events-key`)

**Schema ID**: Globally unique integer identifying a schema definition
- Shared across subjects (same schema = same ID)
- Monotonically increasing
- Content-based deduplication

**Schema Version**: Per-subject version number (1, 2, 3...)
- Independent across subjects
- Multiple versions can point to the same schema ID

**Example** (from Quick Reference):
```
Subject: users-value               Subject: orders-value
  Version 1 → Schema ID 42   |       Version 1 → Schema ID 42 (same!)
  Version 2 → Schema ID 58   |
  Version 3 → Schema ID 42   |     (rolled back to original)
```

### Compatibility Modes

> **See [Quick Reference - Compatibility Modes](#compatibility-modes-quick-table)** for quick lookup.

**Transitive vs Non-Transitive**:
- **Non-transitive** (e.g., BACKWARD): Check against previous version only
- **Transitive** (e.g., BACKWARD_TRANSITIVE): Check against all previous versions

**Common Choice**: `BACKWARD` (new consumers can read old data, upgrade consumers first)

### Registry Modes

- **READWRITE** (default): Normal operation
- **READONLY**: Prevents schema modifications
- **IMPORT**: Migration mode (allows explicit schema IDs/versions)

---

## Architecture Overview

> **See [Quick Reference - Core Architecture](#core-architecture-at-a-glance)** for summary.

### Component Layers

```
REST API (handlers)
    ↓
Sharded Store (hash-routed by subject/schema_id)
    ↓
Per-Shard In-Memory Store (full replica)
    ↓
_schemas Topic (single partition, compacted)
```

### Storage Architecture Details

**Topic-Backed Store**:
- Single-partition `_schemas` topic (Kafka-style, compacted)
- Every broker maintains full in-memory replica
- Writes: produce to topic → consume back → apply to memory
- Uses internal Kafka client (not external library)

**Sharding for Performance**:
- Each CPU core = independent shard with full schema copy
- Requests hash-routed to specific shard
- Write coordination via shard 0 ("reader shard")
- Lock-free parallelism for independent operations

---

## How It Works: Internal Design

> **See [Quick Reference - Key Operations Flow](#key-operations-flow)** for operation summaries.

### Writer Synchronization Details

**Problem**: Multiple nodes writing to single-partition topic concurrently.

**Solution**: Offset-as-sequence-number with optimistic concurrency.

**Protocol**:
1. `read_sync()` - ensure local store is current
2. Produce to `_schemas` expecting offset = `latest_offset + 1`
3. Consume own write from topic
4. Apply to in-memory store
5. Check: `actual_offset == expected_offset`?
   - Yes → Success, return to client
   - No → Another writer won, retry from step 1

**Why consume-own-write**: Guarantees client sees schema only after it's durably stored AND visible in memory.

**Example - Conflict & Retry**:
```
Node A writes subject "foo" expecting offset 1 → lands at offset 1 ✅
Node B writes subject "bar" expecting offset 1 → lands at offset 2 ❌
  (Node A won the race)
Node B retries expecting offset 2 → lands at offset 2 ✅
```

**Key Insight**: Kafka offset = sequence number, giving total ordering without distributed locks.

### Read Paths

**Fast Reads** (single schema): Serve from memory, fallback to sync if missing
**Slow Reads** (lists): Explicit sync before serving for consistency

### Schema ID Allocation

- Allocate locally from in-memory state: `max_existing_id + 1`
- Check for duplicate schema first (reuse existing ID if found)
- On write conflict, retry with updated state

### Soft vs Hard Delete

**Soft Delete**: Marks as deleted, data remains (recoverable)
**Hard Delete**: Writes tombstones, compaction removes data (permanent)
- Hard delete requires prior soft delete
- Tombstones use original sequence numbers for correct compaction

---

## Schema Processing: Sanitization, Canonicalization, and Normalization

> **See [Quick Reference - Processing Pipeline](#processing-pipeline)** for summary table.

These terms are often confused. Here's the breakdown:

### Sanitization
Validate syntax, remove whitespace, compact to consistent format.

**Example**: `{"type": "string",  "extra":  "whitespace"}` → `{"type":"string"}`

### Canonicalization
**Complete pipeline**: Sanitize → optionally normalize → resolve references → store.

This is what turns user input into the final stored form.

### Normalization (Optional)
Sort schema elements deterministically for deduplication.

**Why**: Two semantically identical schemas with different element ordering → different schema IDs (wasteful)

**Example** (Protobuf extensions):
```protobuf
// Different ordering, same semantics
extensions 100 to 199; extensions 50 to 99;  // Schema A
extensions 50 to 99; extensions 100 to 199;  // Schema B

Without normalization: Different schema IDs ❌
With normalization: Same schema ID ✅
```

**Configuration**: `schema_registry_always_normalize` (default: false)

---

## Schema Formats

Three formats supported: **Avro**, **Protobuf**, **JSON Schema**

**Avro**: Compact binary, rich types, strong ecosystem (Hadoop, Spark)
**Protobuf**: Language-neutral, code generation, Google-backed
**JSON Schema**: Human-readable, gradual typing, REST API friendly

### Schema References

> **See [Quick Reference - References](#references)** for support matrix.

**Internal references** (pointing to other schemas in registry): Fully supported for all formats.

**External references** (pointing to public URIs): Not supported for JSON Schema, limited for Avro/Protobuf.

References specify: `{name, subject, version}`

---

## Compatibility Levels

> **See [Quick Reference - Compatibility Modes](#compatibility-modes-quick-table)** for quick lookup.

**Backward** (most common): New schema reads old data → upgrade consumers first
**Forward**: Old schema reads new data → upgrade producers first
**Full**: Both directions → any upgrade order

**Transitive**: Check against all versions (stricter) vs previous version only

### How Checking Works

1. Retrieve compatibility level (subject or global)
2. Determine versions to check (latest vs all, based on transitive)
3. Load reader/writer schemas
4. Format-specific compatibility check
5. Return pass/fail with detailed errors

**Implementation**: Each format (Avro, Protobuf, JSON) has specialized compatibility checkers.

---

## Common Operations

> **See [Quick Reference - REST API Cheat Sheet](#rest-api-cheat-sheet)** for endpoint list.

### Registration Flow

```bash
POST /subjects/{subject}/versions
Body: {"schema": "...", "schemaType": "AVRO"}
→ Returns: {"id": 42}
```

**Steps**: Validate → check compatibility → deduplicate → allocate ID → write to topic → return

### Retrieval

```bash
GET /schemas/ids/{id}           # By schema ID (fast, from memory)
GET /subjects                   # List subjects (slow, syncs topic)
GET /subjects/{subject}/versions/{version}  # Specific version (fast)
```

### Compatibility Check

```bash
POST /compatibility/subjects/{subject}/versions/latest
Body: {"schema": "..."}
→ Returns: {"is_compatible": true/false, "messages": [...]}
```

### Configuration

```bash
PUT /config                     # Global compatibility
PUT /config/{subject}           # Per-subject compatibility
```

---

## Advanced Topics

> **See [Quick Reference - Integration Points](#integration-points)** for summary.

### Future: Contexts (Planned)
- Schema IDs unique per context (namespace), not globally
- GUID for cross-context identity
- Multi-tenancy support

### Integrations

**Authentication**: HTTP Basic, mTLS, SASL/SCRAM, OIDC (Enterprise)

**Authorization** (Enterprise): Per-subject ACLs, operations: `SUBJECT_READ/WRITE/DELETE`, `GLOBAL_CONFIG_*`, audit logging

**Iceberg**: Schema evolution for Iceberg tables with compatibility checking

**Data Transforms (Wasm)**: Schema-aware transformations retrieve schemas by ID

**Tiered Storage**: `_schemas` topic archived to object storage (S3, GCS, Azure)

### Schema ID Validation (Enterprise)

Server-side validation of schema IDs in messages before writing to topic.

**Validation Modes**: `none` (default) | `redpanda` | `compat`

### Metrics

- `vectorized_schema_registry_cache_schema_count`: Schemas in memory
- `vectorized_schema_registry_cache_subject_count`: Subjects (by deleted status)
- `vectorized_schema_registry_cache_subject_version_count`: Versions per subject
- `vectorized_schema_registry_cache_schema_memory_bytes`: Memory usage

### Import Mode

For migrating from another registry:
1. Set mode to `IMPORT`
2. Register with explicit IDs/versions
3. Switch back to `READWRITE`

**Purpose**: Preserve schema IDs from source registry for client compatibility.

### Performance Characteristics

**Reads**: In-memory < 100μs | Topic sync ~10ms
**Writes**: Uncontended 20-50ms | Contended 50-200ms (retries)
**Memory**: ~1-10 KB per schema, 10K schemas ≈ 10-100 MB RAM
**Scale**: Tested with 100,000+ schemas, bottleneck is `_schemas` topic throughput

### Topic Compaction

`_schemas` uses log compaction:
- Retains only latest value per key
- Tombstones (hard deletes) trigger removal
- Automatic, no configuration needed

---

### Deployment

**Embedded Architecture**: Built into Redpanda brokers (not standalone service)
- No separate deployment needed
- Every broker serves requests
- Load balance across all brokers

**High Availability**:
- No single point of failure (any broker serves reads/writes)
- Durability via `_schemas` topic replication factor

**Scaling**:
- Vertical: More CPU cores → more shards → higher throughput
- Horizontal: More brokers → distributed HTTP load
- All brokers maintain full schema catalog (no partitioning)

---

## References & Components

### External Documentation
- **Redpanda Docs**: https://docs.redpanda.com/current/manage/schema-reg/
- **Confluent API Reference**: https://docs.confluent.io/platform/current/schema-registry/develop/api.html
- **Internal README**: `src/v/pandaproxy/schema_registry/README.md`

### Key Components in Codebase

| Component | Purpose |
|-----------|---------|
| **types** | Core type definitions (schema, subject, compatibility, modes) |
| **store** | In-memory storage (schemas, subjects, versions, configs) |
| **sharded_store** | Distributed storage across CPU shards |
| **handlers** | REST API endpoint implementations |
| **api** | Top-level orchestration and lifecycle management |
| **compatibility** | Schema compatibility checking (format-specific) |
| **validation** | Schema validation (syntax, semantics, references) |
| **seq_writer** | Sequenced writes with optimistic concurrency |
| **avro / protobuf / json** | Format-specific schema processing |

---

**Last Updated**: 2025-12-08
