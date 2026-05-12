# SR Shadow Link POC — Design

Status: Approved for overnight POC on branch `slsr/poc`.
Owner: Gellert Peresztegi-Nagy.
Scope: minimum viable in-tree vertical slice that proves Confluent SR → Redpanda SR replication can ride on top of the existing `cluster_link` task framework.

## Why

E-199 asks for continuous, ID-preserving replication of a Confluent SR into the Redpanda SR as part of shadow linking. Before writing an RFC, we want a working spike that:

1. Validates the cluster_link::task framework is a good fit for SR replication (vs. inventing parallel plumbing).
2. Answers the riskiest open question from `notes/sl-sr/POC.md`: slowest-path replication performance with sub-1s discovery of new subjects during tailing.
3. Surfaces concrete code shapes for the eventual RFC, especially around mode/config replication, validation, and error reporting.

## Requirements addressed (E-199)

| Requirement | POC coverage |
|---|---|
| Continuous replication of subjects, versions, configs | Yes |
| HTTP Basic auth to source SR | Yes |
| HTTPS to source SR | Yes (Basic over TLS via existing http::client) |
| Subject filtering (wildcards) | `include_regex` field, default `.*` |
| Original subject names + version IDs preserved | Yes (POST with explicit id, dest in IMPORT mode) |
| Validation step; reject + error on validation failure | Yes (local parse + dest POST result) |
| Per-link status (synchronized / pending / error) | Yes (cluster_link::task::state) |
| Per-context/subject status (stretch) | Deferred |
| mTLS, bearer | Deferred |
| Context mapping | Deferred |

## Code shape

```
src/v/cluster_link/schema_registry_replicator_task.{h,cc}   # new task
src/v/cluster_link/model/types.h                            # +schema_registry_replication_config
src/v/cluster_link/link.cc                                  # construct + wire task
tests/rptest/services/confluent_schema_registry.py          # from stash@{1}
tests/docker/ducktape-deps/confluent-schema-registry        # from stash@{1}
tests/rptest/tests/confluent_sr_migration_test.py           # extended
docs/plans/2026-05-12-sl-sr-poc-design.md                   # this doc
```

The new task inherits `cluster_link::task` and follows `group_mirroring_task`'s pattern: `run_impl`, `is_enabled`, `should_start_impl` (true only on shard 0 of the SR leader — matches `seq_writer::reader_shard`), `update_config`, `get_status_report`. Run interval ~250ms during tailing.

## Data flow per tick

```
[src Confluent SR]                       [dest Redpanda SR]
GET /subjects                  ──┐
GET /config (+ per-subject)      │   diff against
GET /mode   (+ per-subject)      │   in-memory snapshot
GET /subjects/X/versions         │
GET /schemas/ids/<id>          ──┘
                                 │
                          topo-sort schemas by references
                                 │
                          local validate (avro/proto/json parse)
                                 │
                          PUT  /mode/{subject}     {"mode":"IMPORT"}    (lazy, idempotent)
                          POST /subjects/X/versions {id, schemaType, schema, references}
                          PUT  /config/{subject}                        (delta)
                                 │
                          update task_state → synchronized
```

## Key design choices

1. **HTTP loopback to destination.** The task POSTs to the destination Redpanda SR over HTTP. Slightly slower than in-process calls into `seq_writer`, but: (a) matches the eventual public design, (b) gets validation for free (the SR handler validates on write), (c) gives realistic latency measurements. Confirmed by user.

2. **ID preservation via the existing wire format.** `POST /subjects/X/versions` accepts an `{"id": N}` body; the handler plumbs that into `stored_schema{...id=N}` → `seq_writer::write_subject_version`, which writes at that ID. No SR-internal changes needed. Requires the subject (or globally) to be in IMPORT mode.

3. **Explicit IMPORT mode, not implicit.** The task issues `PUT /mode/{subject}` body `{"mode":"IMPORT"}` lazily on first write. The implicit-IMPORT idea — injecting a `sequence_state_checker` into the destination SR that returns `writes_disabled() = true` while shadowing — is the right long-term hook (file: `src/v/pandaproxy/schema_registry/seq_writer.h:26-37`) but out of scope for tonight.

4. **Validation strategy.** Two layers. (a) Local: parse the schema body using the same parsers the dest SR uses (`avro::make_schema`, `protobuf::make_schema`, `json::make_schema`) before sending. (b) Trust the dest's `POST` to do canonical validation. On validation failure: skip the schema, emit `vlog(...warn)`, increment an error counter, set task state to `error`. Continue replicating others — halting on first error would block all 60k schemas on one bad input.

5. **Reference DAG ordering.** Topo-sort schemas by references before write. Best-effort if cycles are present; retry-on-fail fallback.

6. **Continuous + <1s discovery.** Two-phase loop:
   - Catch-up: one-shot at task start. Paginate `GET /subjects`, fetch all versions+schemas, write all. Track high-water in memory.
   - Tail: every ~250ms. `GET /subjects` only, diff to find new subjects, prioritize those; revisit known subjects on a longer interval (~5s) for new versions.

7. **Status / errors.** Use `task::state` and `task::get_status_report()` directly — these already plumb to admin endpoints. Map `synchronized` → `running`, `pending` → `starting`/`paused`, `error` → `unavailable`/explicit error state.

8. **Subject filtering.** Single `include_regex` field on the config; honored client-side after `GET /subjects`. Exclude regex + context mapping deferred.

## Test plan (overnight harness)

Extend `confluent_sr_migration_test.py` from stash@{1}:

1. Seed N=20 schemas across 5 subjects in Confluent SR.
   - Mix of Avro + JSON Schema
   - One schema with references (to test topo sort)
   - One intentionally invalid (to test validation-rejection)
2. Enable the shadow link with SR replication task pointed at Confluent SR via admin API.
3. Assert all 20 valid schemas appear in dest with **identical IDs**.
4. Assert the invalid schema is rejected; task state reports `error`.
5. Create a new schema in source; assert it appears in dest within 1s.
6. Change compat level on source; assert mirrored.

## Explicit non-goals tonight

- Implicit IMPORT mode (sequence_state_checker injection)
- mTLS, bearer tokens, OIDC
- Context mapping / context remapping
- Per-subject status reporting (link-level only)
- Large-scale memory / fragmentation measurement (defer once the task exists)
- Persistent checkpointing (task restarts re-scan source; inserts are idempotent)
- Hard/soft delete propagation
- ID translation (`translate_ids`)
- Glue / MSK source auto-detection

## Risks

- **Reference DAG cycles.** SR allows version cycles. Mitigation: best-effort topo + retry-on-fail.
- **CP 7.7.1 tarball download.** Slow first build (~600MB). Cached afterward by ducktape-deps.
- **Admin API for SR replication config.** `shadow_linking_rpc.json` may not have an SR replication block today. Plan: extend it; if that proves expensive overnight, fall back to enabling the task unconditionally when an `sr_source.url` is present in link config.
- **Overnight compile loop.** Redpanda builds are slow. Mitigation: incremental builds, conservative scope, leave clear TODOs rather than chase perfection at 3am.

## Out-of-scope (for the RFC, not the POC)

- Distributed sorting across shards/workers
- Performance under hot-subject contention
- Rate limiting to source (RPCN default: 10 req/s)
- Failover integration
- IMPORT mode auto-toggle + restore
- Hub-and-spoke / translate_ids design
