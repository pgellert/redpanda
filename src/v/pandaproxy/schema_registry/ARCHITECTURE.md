# Schema Registry Architecture

**Last Updated:** December 2025
**Target Audience:** Engineers and managers onboarding to Schema Registry subsystem

---

## What Problem Does This Solve?

In distributed streaming systems, producers and consumers exchange data through topics. As systems evolve, data schemas change—fields are added, removed, or modified. Without coordination, these changes break consumers. A single incompatible change can cause widespread data corruption or service outages.

**Schema Registry solves three critical problems:**

1. **Schema Evolution Without Downtime** - Enables coordinated schema changes across distributed services without requiring synchronized deployments
2. **Runtime Data Validation** - Prevents malformed data from entering the system by validating against registered schemas
3. **Semantic Versioning of Data Contracts** - Provides a single source of truth for what data shapes are valid at any point in time

Schema Registry acts as a centralized repository that validates, stores, and serves schema definitions in multiple formats (Avro, Protobuf, JSON Schema). It enforces compatibility rules to ensure new schema versions can coexist with old ones.

---

## Core Concepts

Before diving into architecture, understand these fundamental concepts:

### Subject

A **subject** is a namespace for schema versions. By convention, subjects follow the pattern `{topic-name}-key` or `{topic-name}-value`, though any naming convention works.

```
user-events-value  → contains schemas for user event values
user-events-key    → contains schemas for user event keys
```

Each subject maintains an independent version history. Schemas evolve within subjects, not globally.

### Schema Version

Within a subject, schemas evolve through **versions** numbered sequentially: 1, 2, 3, ... Each version is immutable once registered. Version numbers are densely packed—no gaps.

```
user-events-value
  ├─ v1: {name: string}
  ├─ v2: {name: string, email: string}         # added field
  └─ v3: {name: string, email: string, age: int} # added another
```

### Schema ID

Every unique schema definition receives a globally unique **schema_id**. IDs are monotonically increasing integers starting from 1. Multiple subjects can share the same schema_id if they use identical definitions.

```
schema_id: 42 → {type: "record", fields: [...]}
  ↑ used by both "orders-value" v1 and "refunds-value" v1
```

**Design rationale:** Producers embed schema_id in messages, not the full schema. Consumers fetch the schema once by ID and cache it. This dramatically reduces message payload size.

### Compatibility Modes

**Compatibility modes** define what schema changes are allowed. They answer: "Can producers using schema X communicate with consumers using schema Y?"

| Level | Direction | Scope | Definition | Use Case |
|-------|-----------|-------|------------|----------|
| **NONE** | - | - | No validation | Development/testing only |
| **BACKWARD** | Reader→Writer | Latest | New schema reads old data | Add optional fields; consumers upgrade first |
| **BACKWARD_TRANSITIVE** | Reader→Writer | All | New schema reads all old data | Strict backward compatibility for data lakes |
| **FORWARD** | Reader→Writer | Latest | Old schema reads new data | Remove fields; producers upgrade first |
| **FORWARD_TRANSITIVE** | Reader→Writer | All | Old schema reads all new data | Strict forward compatibility |
| **FULL** | Both | Latest | BACKWARD + FORWARD | Safest; any upgrade order |
| **FULL_TRANSITIVE** | Both | All | BACKWARD_TRANSITIVE + FORWARD_TRANSITIVE | Maximum safety across all versions |

**Key insight:** The `_TRANSITIVE` suffix means "check against ALL versions in history" rather than just the most recent version. There are 7 distinct compatibility levels, not 5—TRANSITIVE is a modifier applied to BACKWARD, FORWARD, and FULL modes.

Compatibility is configurable globally or per-subject. Subject-level settings override global defaults.

### Mode (Import/ReadOnly/ReadWrite)

**Mode** controls write operations on subjects:

- **READWRITE** - Normal operation, schemas can be registered
- **READONLY** - Schemas can be read but not modified; useful for production lockdown
- **IMPORT** - Allows importing schemas with specific IDs/versions from another registry

**Note:** The string representation in the API uses `READWRITE` and `READONLY` (no underscores), while the internal enum uses `read_write` and `read_only` (with underscores).

### Schema References

Schemas can **reference** other schemas, enabling composition:

```json
{
  "type": "record",
  "fields": [{
    "name": "address",
    "type": "Address"  ← references another schema
  }],
  "references": [{
    "name": "Address",
    "subject": "address-value",
    "version": 1
  }]
}
```

References ensure schemas are validated together and prevent dangling dependencies.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        HTTP/REST API                             │
│                    (Confluent-Compatible)                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────┐     ┌──────────────┐     ┌────────────────┐   │
│  │  Handlers   │────▶│   Service    │────▶│  Seq Writer    │   │
│  │             │     │              │     │   (Shard 0)    │   │
│  │ HTTP Routes │     │  Auth/Authz  │     │                │   │
│  └─────────────┘     │  Validation  │     │  Write Coord.  │   │
│                      └──────────────┘     └────────┬───────┘   │
│                                                     │           │
│                                           Produce   │           │
│                                                     ▼           │
│                      ┌──────────────────────────────────────┐  │
│                      │   _schemas Kafka Topic      │  │
│                      │   (Single Partition, Compacted)      │  │
│                      └──────────────┬───────────────────────┘  │
│                                     │ Replay/Consume           │
│                                     ▼                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │            Sharded Store (In-Memory)                      │  │
│  │                                                            │  │
│  │  Shard 0         Shard 1         Shard 2    ...  Shard N  │  │
│  │  ┌────────┐      ┌────────┐     ┌────────┐     ┌────────┐│  │
│  │  │ Store  │      │ Store  │     │ Store  │     │ Store  ││  │
│  │  │        │      │        │     │        │     │        ││  │
│  │  │Subject │      │Subject │     │Subject │     │Subject ││  │
│  │  │Schemas │      │Schemas │     │Schemas │     │Schemas ││  │
│  │  └────────┘      └────────┘     └────────┘     └────────┘│  │
│  │       ▲               ▲              ▲              ▲     │  │
│  └───────┼───────────────┼──────────────┼──────────────┼─────┘  │
│          │               │              │              │        │
│          └───────── Hash(subject) ──────┴──────────────┘        │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Key Architectural Characteristics

**Distributed State via Kafka Topic**
All mutations are written to a single-partition Kafka topic (`_schemas`). Every node reads from this topic to build identical in-memory state. This provides:
- Single source of truth across cluster
- Natural disaster recovery via topic backup
- Multi-datacenter replication via topic mirroring
- No distributed consensus required

**Sharded In-Memory Storage**
Subjects are hash-partitioned across CPU cores for parallelism. Each shard maintains a B-tree index of its assigned subjects. Read operations are local to the owning shard (NUMA-friendly).

**Write Coordination via Sequencing**
All writes funnel through shard 0. The sequencer embeds offset + node_id in records to detect write collisions. When two nodes simultaneously write to the same subject, one detects the collision and retries. This enables multi-master writes without distributed locking.

**Multi-Format Validation**
Schema format handlers (Avro, Protobuf, JSON Schema) validate schemas using native libraries. Each format has distinct validation, canonicalization, and compatibility checking logic.

---

## Component Architecture

### Layer 1: API Entry (`api.h`)

**Responsibility:** Lifecycle management and dependency injection

The `api` class orchestrates startup/shutdown and wires together all major components:

- Kafka client for topic operations
- Sharded store for schema storage
- Service layer for HTTP handling
- Seq writer for write coordination
- Schema ID cache for performance
- Security integrations (auth, authz, audit)

**Key Operations:**
- `start()` - Initialize all subsystems, replay topic into memory
- `stop()` - Graceful shutdown with connection draining
- `restart()` - Used for config changes without full restart

### Layer 2: Service & Handlers (`service.h`, `handlers.h`)

**Responsibility:** HTTP request processing and business logic

**Service** acts as a gateway:
- Registers HTTP routes
- Performs authentication (SASL, mTLS, etc.)
- Performs authorization (ACL checks per resource)
- Delegates to handlers
- Manages request/response lifecycle

**Handlers** implement REST endpoints:
- Schema CRUD: register, retrieve, delete
- Subject management: list, delete, query
- Configuration: compatibility levels, modes
- Compatibility checking: test without registering
- Discovery: find schemas by ID, find subjects using a schema

All handlers follow a uniform signature: `(request, reply, auth_result) → future<reply>`. This enables consistent error handling and middleware patterns.

### Layer 3: Write Coordination (`seq_writer.h`)

**Responsibility:** Serialize writes and synchronize cluster state

**Critical constraint:** All writes execute on shard 0 only.

**Sequencing Protocol:**

1. Acquire write semaphore (single writer at a time)
2. Call `read_sync()` to ensure latest topic offset is applied
3. Calculate next offset = current + 1
4. Execute operation (e.g., register schema)
5. Produce record to topic with offset + node_id
6. Apply mutation to local store
7. If collision detected (different record at same offset), retry with exponential backoff

**Collision Detection:**
Each record contains `seq_marker`:
```cpp
struct seq_marker {
    std::optional<model::offset> seq;    // Kafka offset
    std::optional<model::node_id> node;  // Which node wrote
    schema_version version;
    seq_marker_key_type key_type;
};
```

If two nodes write at offset 100, both produce records. The second node to apply offset 100 sees a mismatch (different node_id) and retries.

**Design rationale:** This avoids distributed consensus (Raft, Paxos) while enabling multi-master writes. Trade-off: writes are serialized through shard 0, limiting write throughput. For Schema Registry's typical workload (infrequent writes, frequent reads), this is acceptable.

### Layer 4: Storage (`sharded_store.h`, `store.h`)

**Sharded Store** - Distributes subjects across shards

```cpp
ss::shard_id shard_for(const subject& sub) {
    auto hash = xxhash_64(sub().data(), sub().length());
    return jump_consistent_hash(hash, ss::smp::count);
}
```

Operations:
- Hash subject → determine owning shard
- Submit operation to that shard via `ss::submit_to()`
- Shard executes operation on local `store` instance
- Return result

**Store** - Single-shard schema index

```cpp
class store {
    // Subject name → subject metadata
    absl::btree_map<subject, subject_data> _subjects;

    // Schema ID → schema definition
    absl::btree_map<schema_id, schema_metadata> _schemas;

    // Schemas pending compilation (after topic replay)
    chunked_vector<schema_id> _marked_schemas;

    // Global defaults
    compatibility_level _compatibility{compatibility_level::backward};
    mode _mode{mode::read_write};
    is_mutable _mutable;

    // Plus: metrics fields
};
```

**Schema ID Allocation:** Schema IDs are allocated sequentially using the B-tree's maximum key + 1. The `sharded_store` class (not individual `store` instances) tracks `_next_schema_id` globally on shard 0.

**Subject Data:**
```cpp
struct subject_data {
    chunked_vector<subject_version_entry> versions;  // Version history
    is_deleted deleted;                               // Soft delete flag
    compatibility_level compat;                       // Per-subject compat
    mode mode;                                        // Per-subject mode
    chunked_vector<seq_marker> written_at;            // Write history
};
```

**Lookup Complexity:**
- Get schema by ID: O(log n) B-tree lookup
- Get schema by subject+version: O(log n) B-tree + O(log v) version lookup
- List subjects: O(n) full scan with filter
- Check compatibility: O(m × s) where m = versions to check, s = schema complexity

### Layer 5: Schema Format Handlers (`avro.h`, `protobuf.h`, `json.h`)

**Responsibility:** Format-specific validation, canonicalization, and compatibility

Each handler implements:

| Operation | Purpose |
|-----------|---------|
| `make_*_schema_definition()` | Parse raw schema, validate syntax |
| `make_canonical_*_schema()` | Normalize to canonical form (deterministic) |
| `format_*_schema_definition()` | Convert to output format (pretty, resolved, etc.) |
| `check_compatible()` | Determine if reader schema can decode writer schema |

**Avro Example:**
- Uses Apache Avro C++ library
- Validates field types, record structure, unions
- Canonicalization: sorts fields, resolves references
- Compatibility: checks field defaults, type promotions (int → long)

**Protobuf Example:**
- Uses Google Protocol Buffers library
- Parses `.proto` syntax into FileDescriptor
- Supports import/reference resolution
- Compatibility: field number immutability, label changes (required/optional)

**JSON Schema Example:**
- Validates against JSON Schema Draft 7 meta-schema
- Compatibility: property type narrowing, constraint tightening

**Valid Schema Abstraction:**
All formats are wrapped in `std::variant`:

```cpp
class valid_schema {
    std::variant<
        avro_schema_definition,
        protobuf_schema_definition,
        json_schema_definition
    > _impl;

    // Visitor pattern enables format-agnostic operations
    template<typename V>
    decltype(auto) visit(V&& visitor);
};
```

This allows the rest of the system to operate on schemas without knowing their format.

### Layer 6: Compatibility Checking (`compatibility.h`)

**Responsibility:** Validate schema changes against compatibility rules

**Compatibility Check Flow:**

```
is_compatible(version, new_schema, verbose)
  ↓
Get compatibility level (subject or global)
  ↓
Determine which versions to check
  ├─ Non-transitive: versions ≥ provided version
  └─ Transitive: all versions
  ↓
For each version:
  check_compatible(new_schema, old_schema, direction)
    ↓
  Format-specific compatibility checker
    ↓
  Return detailed incompatibilities if verbose=true
```

**Incompatibility Types** (examples):

| Format | Incompatibility Examples |
|--------|-------------------------|
| Avro | `type_mismatch`, `missing_default_value`, `fixed_size_changed` |
| Protobuf | `field_kind_changed`, `required_field_added`, `oneof_field_removed` |
| JSON | `type_narrowed`, `max_length_decreased`, `property_added_to_closed_model` |

Each incompatibility includes:
- Error type (enumerated)
- Path in schema (e.g., `/fields/address/type`)
- Descriptive message

This granular reporting helps developers understand why a schema change was rejected.

---

## Data Flow Patterns

### Schema Registration Flow

```
POST /subjects/{subject}/versions
  │
  ├─ Parse request body
  │  └─ Extract: schema, schemaType, references, id (optional), version (optional)
  │
  ├─ Validate schema format
  │  └─ Call make_*_schema_definition() for Avro/Protobuf/JSON
  │
  ├─ Canonicalize schema
  │  └─ Normalize field order, resolve references
  │
  ├─ Check if schema already exists
  │  ├─ YES: return existing schema_id + version
  │  └─ NO: continue
  │
  ├─ Check mode mutability
  │  └─ Reject if subject is in READ_ONLY mode
  │
  ├─ Check compatibility
  │  ├─ Get compatibility level (subject or global)
  │  ├─ Get versions to check (transitive = all, else = recent)
  │  └─ Validate new schema against each version
  │
  ├─ Write via seq_writer
  │  ├─ Acquire write semaphore (shard 0)
  │  ├─ Produce record to _schemas topic
  │  ├─ Apply to local sharded_store
  │  └─ Return schema_id + version
  │
  └─ Return response: {id: 42, version: 3}
```

**Key Design Decision:** Compatibility checking happens *before* writing to the topic. Invalid schemas are rejected immediately, preventing bad data from entering the log.

### Schema Retrieval Flow

```
GET /subjects/{subject}/versions/{version}
  │
  ├─ Call sharded_store.get_subject_schema()
  │  │
  │  ├─ Hash subject → determine shard_id
  │  │
  │  ├─ Submit to shard: container().invoke_on(shard_id, ...)
  │  │
  │  └─ Shard executes:
  │     └─ store.get_subject_schema(subject, version, include_deleted)
  │        ├─ Lookup subject in _subjects B-tree
  │        ├─ Find version in versions vector
  │        └─ Lookup schema definition by schema_id in _schemas B-tree
  │
  └─ Return: {subject, version, id, schema, schemaType, references}
```

**Performance characteristic:** Read operations are local to one shard (no cross-shard coordination). B-tree lookups are O(log n). Typical latency: sub-millisecond.

### Write Collision Resolution

```
Node A                          Node B
  │                               │
  ├─ Write to "users-value"       ├─ Write to "users-value"
  │  (both start at offset 99)    │
  │                               │
  ├─ Produce with seq=100, node=A │
  │                               ├─ Produce with seq=100, node=B
  │                               │
  ├─ Apply locally (offset 100)   │
  │                               ├─ Apply locally (offset 100)
  │                               │
  ├─ Read offset 100 from topic   ├─ Read offset 100 from topic
  │                               │
  ├─ Check: seq=100, node=A       ├─ Check: seq=100, node=A
  │  ✓ Matches local write        │  ✗ Mismatch! Expected node=B
  │                               │
  │                               ├─ Throw write_collision exception
  │                               │
  │                               ├─ Retry with backoff
  │                               │
  │                               └─ Next attempt uses seq=101
```

**Trade-off:** Write collisions cause retries, increasing latency. In practice, schema registration is infrequent enough that collisions are rare.

---

## Key Design Decisions

### Why Kafka Topic for Persistence?

**Decision:** Use `_schemas` topic as system of record instead of local storage (RocksDB, SQLite, etc.)

**Rationale:**

| Benefit | Explanation |
|---------|-------------|
| Single source of truth | All nodes converge to identical state by replaying topic |
| Natural disaster recovery | Topic backups = schema registry backups |
| Multi-datacenter replication | Mirror topic across DCs for geo-redundancy |
| No distributed consensus | Avoids complexity of Raft/Paxos for state machine replication |
| Leverages existing infra | Reuses Kafka cluster's durability, replication, compaction |

**Trade-off:** Startup latency increases with topic size. Mitigated by compaction and efficient replay.

### Why Per-Subject Sharding?

**Decision:** Hash-partition subjects across CPU cores using jump consistent hash

**Rationale:**

| Benefit | Explanation |
|---------|-------------|
| Parallelism | Read operations scale with core count |
| NUMA-friendly | Data locality on multi-socket systems |
| Fault isolation | Corruption in one shard doesn't affect others |
| Simple load balancing | No need for explicit partitioning strategy |

**Trade-off:** Cross-shard operations (e.g., list all subjects) require scatter-gather. Acceptable since these are administrative operations.

### Why Sequencing Pattern?

**Decision:** Embed offset + node_id in topic records to detect collisions

**Rationale:**

| Benefit | Explanation |
|---------|-------------|
| Multi-master writes | Any node can write without coordination |
| Idempotent replay | Same record applied twice = no-op |
| Collision detection | Two concurrent writes to same subject detected automatically |
| No external coordination | No Zookeeper, etcd, or distributed lock service needed |

**Trade-off:** All writes serialized through shard 0. Limits write throughput to ~thousands/sec. Sufficient for typical schema registry workloads (far lower than topic throughput).

### Why Separate Format Handlers?

**Decision:** Distinct code paths for Avro, Protobuf, JSON Schema

**Rationale:**

| Benefit | Explanation |
|---------|-------------|
| Format-specific validation | Each format has unique syntactic/semantic rules |
| Native library integration | Use official parsers (Avro C++, protoc, etc.) |
| Clear error messages | Format-aware errors help developers debug |
| Independent evolution | Can upgrade one format handler without affecting others |

**Trade-off:** Code duplication for common operations (parsing, storage). Mitigated by visitor pattern and shared abstractions.

### Why In-Memory Storage?

**Decision:** Keep all schemas in RAM, backed by Kafka topic

**Rationale:**

| Benefit | Explanation |
|---------|-------------|
| Latency | Sub-millisecond lookups for hot path (schema ID → definition) |
| Simplicity | No cache invalidation, no disk I/O in request path |
| Scalability | Schema corpus typically <1GB even for large deployments |

**Trade-off:** Memory footprint grows with schema count. Mitigated by compaction and schema deduplication (same definition = same ID).

**Capacity planning:** Assume ~10KB per schema × 100,000 schemas = ~1GB. Even aggressive installations stay under 10GB.

---

## Integration Points

### Kafka Cluster

**Dependencies:**
- Kafka client for producing/consuming `_schemas` topic
- Cluster controller for topic metadata
- Topic auto-creation (configurable replication factor)

**Topic Configuration:**
- Single partition (ensures total ordering)
- Compacted (retains latest value per key)
- Configurable retention (typically infinite)
- Configurable replication factor

### Security & Authorization

**Authentication:**
- Supports SASL (PLAIN, SCRAM, GSSAPI), mTLS, OAuth
- Validates credentials via `request_authenticator`

**Authorization:**
- ACL-based resource model
- Resources: global `Registry`, per-subject `Subject:{name}`
- Operations: `Read`, `Write`, `Delete`, `Alter`, `Describe`

**Example ACL:**
```
Principal: User:alice
Resource: Subject:orders-value
Operations: Read, Write
```

**Audit Logging:**
- Logs all operations to audit log manager
- Includes: principal, operation, resource, timestamp, outcome

### Schema ID Validation (Integration with Kafka)

Schema Registry integrates with Kafka brokers to validate records at produce time:

```
Producer → Kafka Broker
              ↓
       Schema ID in record?
              ↓
       Fetch schema from Schema Registry
              ↓
       Validate record against schema
              ↓
       Reject if invalid, accept if valid
```

This prevents malformed data from entering topics.

### Monitoring & Metrics

**Prometheus Metrics Exported:**

| Metric | Description |
|--------|-------------|
| `schema_registry_cache_schema_count` | Total unique schemas |
| `schema_registry_cache_subject_count` | Total subjects (by deleted status) |
| `schema_registry_cache_subject_version_count` | Versions per subject |
| `schema_registry_cache_schema_memory_bytes` | Memory used by schemas |
| `vectorized_httpd_connections_count` | Active HTTP connections |

**Logging:**
- Structured logs at INFO, DEBUG, TRACE levels
- Critical paths: schema registration, compatibility failures, write collisions

---

## Understanding Schema Lifecycle

### Registration

1. Client POSTs schema to `/subjects/{subject}/versions`
2. System validates format (Avro/Protobuf/JSON)
3. System canonicalizes schema
4. System checks if schema already exists → reuses schema_id
5. System checks compatibility against existing versions
6. System writes to Kafka topic via seq_writer
7. System applies to in-memory store
8. System returns `{id, version}`

### Evolution

1. Client POSTs new schema version to same subject
2. System validates compatibility (BACKWARD/FORWARD/FULL)
3. If compatible, assigns new version number (incrementing)
4. If incompatible, rejects with detailed error report
5. Schema_id may be new or reused if definition matches existing

### Deletion (Two-Phase)

**Soft Delete:**
```
DELETE /subjects/{subject}/versions/{version}
```
- Marks version as deleted
- Version still in storage, excluded from normal queries
- Can be resurrected by re-registering

**Hard Delete:**
```
DELETE /subjects/{subject}/versions/{version}?permanent=true
```
- Must be preceded by soft delete
- Removes from storage entirely
- Writes tombstone to Kafka topic (key with null value)
- Irreversible

**Design rationale:** Two-phase delete prevents accidental data loss. Soft delete is reversible; hard delete is final.

---

## Reasoning About Changes

### Adding a New Endpoint

1. **Add handler** in `handlers.h/cc`
   - Follow signature: `(request, reply, auth_result) → future<reply>`
   - Extract parameters from request
   - Call service/sharded_store methods
   - Return formatted response

2. **Register route** in `service.cc`
   - Map HTTP method + path to handler
   - Configure auth requirements

3. **Update service methods** if needed
   - Add coordination logic
   - Call seq_writer for mutations

4. **Add tests** in `test/`
   - Test success path
   - Test error paths (not found, invalid input, etc.)

### Adding a New Schema Format

1. **Create format handler** (e.g., `xml_schema.h/cc`)
   - Implement `make_xml_schema_definition()`
   - Implement `make_canonical_xml_schema()`
   - Implement compatibility checker

2. **Add to variant** in `types.h`
   ```cpp
   using valid_schema = std::variant<
       avro_schema_definition,
       protobuf_schema_definition,
       json_schema_definition,
       xml_schema_definition  // ← new
   >;
   ```

3. **Update visitors** throughout codebase
   - Add XML case to all visitor pattern uses

4. **Add to schema_type enum**
   ```cpp
   enum class schema_type { avro, json, protobuf, xml };
   ```

### Modifying Storage Layout

**High risk** - storage format is serialized to Kafka topic. Changes must be backward-compatible.

**Safe changes:**
- Add optional fields to records (default values)
- Add new record types (with version discriminator)

**Unsafe changes:**
- Rename fields (breaks deserialization)
- Change field types (breaks deserialization)
- Remove required fields

**Migration strategy:**
- Write migration code in `storage.h`
- Version records explicitly
- Support N-1 version for rolling upgrades

### Optimizing Compatibility Checks

**Current bottleneck:** Transitive compatibility checks all versions (O(n) where n = version count).

**Optimization strategies:**

1. **Cache compatibility results**
   - Key: (schema1_id, schema2_id, direction)
   - Value: compatible (true/false) + messages
   - Invalidate on schema deletion

2. **Parallel compatibility checks**
   - Check versions in parallel (currently sequential)
   - Aggregate results

3. **Early termination**
   - Stop on first incompatibility (unless verbose=true)

**Trade-offs:** Cache memory vs. CPU time. For subjects with 100+ versions, caching likely worthwhile.

---

## Common Operational Scenarios

### Scenario: Schema Registration Latency Spike

**Symptoms:** `POST /subjects/{subject}/versions` taking 5+ seconds

**Likely causes:**

1. **Write collisions** - Multiple nodes writing to same subject
   - Check logs for `write_collision` exceptions
   - Check metrics for retry count
   - **Mitigation:** Coordinate schema registrations through single node

2. **Topic lag** - `_schemas` topic has high lag
   - Check `_loaded_offset` vs. topic high watermark
   - Check Kafka cluster health
   - **Mitigation:** Increase topic partition count (requires careful migration)

3. **Compatibility check slow** - Many versions to check
   - Check subject version count
   - Check if transitive compatibility enabled
   - **Mitigation:** Use non-transitive compatibility or cache results

### Scenario: Memory Usage Growing

**Symptoms:** Schema Registry process memory increasing over time

**Likely causes:**

1. **Schema proliferation** - Many unique schemas registered
   - Check `schema_count` metric
   - Audit which clients are registering schemas
   - **Mitigation:** Consolidate schemas, use references

2. **Version proliferation** - Many versions per subject
   - Check `subject_version_count` metric
   - **Mitigation:** Delete old versions (soft then hard delete)

3. **Zombie subjects** - Subjects created for testing never cleaned up
   - Check `subject_count` metric
   - **Mitigation:** Delete unused subjects

### Scenario: Startup Time Increasing

**Symptoms:** Schema Registry takes 10+ minutes to start

**Likely cause:** Large `_schemas` topic

- Topic has millions of records
- Replay on startup must process all records
- **Mitigation:** Enable aggressive compaction (reduces record count)

**Compaction effectiveness:**
- Before: 10M records (all history)
- After: 100K records (latest state only)

---

## Compatibility with Confluent Schema Registry

Redpanda Schema Registry aims for API compatibility with Confluent Schema Registry but has some differences:

### Compatible Features

- REST API endpoints (nearly identical)
- Schema formats: Avro, Protobuf, JSON Schema
- Compatibility checking algorithms
- Schema references
- Subject versioning model

### Known Differences

- **Storage:** Redpanda uses native topic, Confluent uses Kafka + local RocksDB
- **Leader election:** Redpanda uses sequencing pattern, Confluent uses Kafka coordinator
- **Performance characteristics:** Different due to architecture differences
- **Some advanced features:** Check Redpanda docs for feature parity matrix

**Migration path:** Schemas can be exported from Confluent and imported to Redpanda using IMPORT mode.

---

## Related Documentation

- **User Documentation:** [Redpanda Schema Registry Docs](https://docs.redpanda.com/current/manage/schema-reg/)
- **API Reference:** [Confluent Schema Registry API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html) (largely compatible)
- **Source Code:** `src/v/pandaproxy/schema_registry/`
- **Tests:** `src/v/pandaproxy/schema_registry/test/`

---

## Summary

Schema Registry is a distributed, strongly consistent schema repository that enables safe schema evolution in streaming systems. Its architecture leverages Kafka for persistence, sharding for parallelism, and sequencing for write coordination. The system validates schemas in multiple formats, enforces compatibility rules, and integrates with Kafka for runtime validation.

**Key takeaways for new engineers:**

1. **Subjects are namespaces** - Each evolves independently
2. **Schema IDs are global** - Deduplication is automatic
3. **Compatibility modes prevent breakage** - Choose wisely per subject
4. **Kafka topic is source of truth** - All nodes converge via replay
5. **Writes are serialized** - Through shard 0 via sequencing pattern
6. **Reads are sharded** - Parallel across cores for performance

When in doubt, read the code starting from `api.h` (lifecycle) → `service.h` (HTTP) → `seq_writer.h` (writes) → `sharded_store.h` (reads) → `store.h` (storage).
