# SR Shadow Link POC — Findings

Branch: `slsr/poc`. Overnight build session, working from
`docs/plans/2026-05-12-sl-sr-poc-design.md` and the implementation
plan `docs/plans/2026-05-12-sl-sr-poc.md`.

## Quick summary

The POC compiles cleanly on top of the existing cluster_link
framework and adds a new `schema_registry_replicator_task` that
replicates a Confluent-compatible Schema Registry into the local
Redpanda SR over HTTP, preserving subject names and schema IDs.

What's in:

- New `schema_registry_sync_config::shadow_via_http_api` variant that
  carries the source URL, optional HTTP Basic credentials, an include
  regex, and tail/version-revisit intervals.
- Reusable `cluster_link::sr_http_client` library wrapping
  `http::abstract_client` with typed methods for the SR endpoints
  actually used by the replicator
  (subjects, versions, schemas-by-version, mode, config; PUT mode,
  PUT config, POST schema-with-id).
- `cluster_link::topo_sort_by_refs` library that orders a batch of
  schemas so referents come before referrers. Stable; falls back
  cleanly on cycles.
- `cluster_link::schema_registry_replicator_task` that drives all of
  the above through a controller-singleton task: catch-up on first
  tick, tail loop afterwards with <1s new-subject discovery.
- Factory registered in `cluster_link/service.cc:1283` so the task
  is constructed alongside the existing ones.
- Unit-tested pure-logic helpers: 9 tests on URL parsing + Basic
  auth encoding, 11 tests on toposort.
- All 12 cluster_link unit tests pass on the new tree. The full
  `bazel build //src/v/redpanda/... //src/v/cluster_link/...` is
  green.

What's deliberately deferred to follow-ups (and why):

- **Admin-API exposure of the new variant.** The
  `shadow_linking_rpc.json` admin surface still only knows about
  `shadow_entire_schema_registry`. A no-op handler was added in
  `admin/shadow_link/converter.cc:1005` so existing clients keep
  working. To exercise the new task end-to-end via ducktape we'll
  need either an admin-API extension or a direct serde injection
  path. Out of scope for one night.
- **Local SR URL discovery.** The destination client is hardcoded to
  `http://127.0.0.1:8081` in
  `schema_registry_replicator_task.cc:33`. Should be sourced from
  `pandaproxy::schema_registry::configuration::schema_registry_api`.
- **mTLS, bearer tokens, HTTPS.** HTTP Basic over plaintext only.
  The transport already supports TLS via
  `net::base_transport::configuration::credentials`; wiring it is
  config plumbing, not protocol work.
- ~~**Compatibility-level replication.**~~ DONE — the task now mirrors
  the global compatibility level and per-subject overrides at
  catch-up time (commit `28654376af`). Per-subject `404` from the
  source is correctly treated as "no override, inherit global" and
  not as an error. Per-tail revisits for compat drift remain a TODO.
- **Per-subject status (E-199 stretch goal).** Counters are
  per-link; per-subject status would need a new map exposed via an
  override of `task::get_status_report()`.
- **Context handling.** Subjects are deserialized as
  `context_subject::unqualified(...)` — i.e. always in the default
  context. Confluent's qualified subject form `":context:name"` is
  parsed by the existing `context_subject::from_string` but we
  don't propagate context information yet.
- **Backoff and rate limiting.** The task does no in-process
  retry — every transient HTTP failure is logged + counted and the
  next tick re-tries the affected subjects. For production we'll
  want at least exponential backoff on `transport` and `5xx`
  errors, and an outgoing rate-limit to protect Confluent Cloud's
  documented 25 wps / 75 rps per LSRC.
- **Mode persistence and restoration.** The task transitions the
  dest into IMPORT mode once and never restores. RPCN migrator
  has the same behaviour; long-term we should record the original
  mode and restore it on link delete / failover.

## File map

```
src/v/cluster_link/model/types.h, types.cc          new variant
src/v/cluster_link/sr_http_client.h, .cc            new HTTP client
src/v/cluster_link/sr_topo_sort.h, .cc              new toposort
src/v/cluster_link/schema_registry_replicator_task.h, .cc  the task
src/v/cluster_link/service.cc                       factory registered
src/v/cluster_link/BUILD                            cc_library entries
src/v/cluster_link/tests/sr_http_client_test.cc     unit tests
src/v/cluster_link/tests/sr_topo_sort_test.cc       unit tests
src/v/cluster_link/tests/BUILD                      cc_gtest entries
src/v/redpanda/admin/services/shadow_link/converter.cc
                                                    non-exhaustive
                                                    visitor fixed
tests/docker/ducktape-deps/confluent-schema-registry,
tests/rptest/services/confluent_schema_registry.py,
tests/rptest/tests/confluent_sr_migration_test.py   prior stash work,
                                                    not yet wired to
                                                    the new task
```

## What the POC validates

1. **The cluster_link::task framework is a good fit.** Inheriting
   from `controller_locked_task` (the same base used by
   `security_migrator`) gave us cluster-wide singleton scheduling,
   start/stop reconciliation, state-change notifications, and
   per-link status plumbing for free. We do not need to invent a
   separate scheduler.
2. **ID preservation is purely a wire-format concern.**
   `post_subject_versions.h:67` accepts `std::optional<schema_id>`
   in the request body and plumbs it into
   `stored_schema{...id=N}` → `seq_writer::write_subject_version`.
   No SR-internal changes were required to support IMPORT-style
   writes from outside the SR.
3. **The variant pattern in `schema_registry_sync_config`
   accommodates cross-vendor sources cleanly.** Adding a new
   `shadow_via_http_api` member to the existing variant did not
   require any changes to the core link configuration logic.
4. **The `http::client` and the `iobuf` machinery handle
   variable-sized schema bodies adequately for ~kB-class payloads.**
   The POC builds and links without special treatment. Whether it
   scales to 10 MB schemas without fragmentation is the next
   measurement we should run on this branch.

## What the POC does NOT yet validate

1. **End-to-end behaviour against a real source SR.** Building
   compiles is not the same as round-tripping a schema. The
   integration test extension from `stash@{1}` is committed but
   not yet wired to the new task; doing so requires the admin-API
   change above.
2. **Performance under load.** The simplest pull strategy
   (GET subjects → for each: GET versions → for each: GET schema)
   is O(subjects × avg-versions) HTTP calls. At Zillow scale
   (~15k subjects, ~60k schemas) and CFLT Cloud's 75 rps rate
   limit, the worst-case catch-up time is roughly 60000 / 75
   ≈ 13 minutes. The faster `GET /schemas?limit=N` path is
   CC-only.
3. **Memory behaviour.** No measurement yet. The single-shard
   model holds the `_seen` map and any in-flight batch on
   shard 0; the design doc anticipates this as the first thing
   to measure once the task is wired up live.
4. **Sub-1s discovery latency.** The tail loop runs every 250ms
   but the actual time-to-discover-and-write a new subject hasn't
   been measured. With one HTTP roundtrip for `GET /subjects` and
   another for `GET /subjects/X/versions/1` we should comfortably
   hit <500ms intra-LAN; CC over WAN with 300ms RTT will be
   1–2s. Worth measuring against the real source.

## Code-reuse opportunities surfaced (not yet acted on)

The user explicitly asked whether the POC could motivate
reorganizations of the SR's internal APIs for better code reuse.
The honest answer is: the boundaries here are healthy, and the POC
mostly speaks the SR's existing public types (`stored_schema`,
`subject_schema`, `schema_definition`, etc.). A few opportunities
worth recording but not acting on tonight:

1. **JSON shapes for SR wire formats.** The destination's
   `post_subject_versions.h` parses the same JSON shape that the
   POC's `sr_http_client::post_schema_with_id` builds. There's a
   small duplication: keys (`id`, `version`, `schema`,
   `schemaType`, `references`, `name`, `subject`) are spelled out
   in both producer and parser. A shared helper library —
   something like `pandaproxy::schema_registry::wire::serialize/parse`
   — would let both sides agree on the wire schema. For the POC
   this is overkill but for production it's the right shape.
2. **A `schema_writer` interface implemented by `seq_writer`.**
   Today the task writes via HTTP loopback to localhost. If we
   wanted a faster in-process path (skipping the HTTP layer
   entirely), we'd want `schema_writer` as an interface that
   both `seq_writer` and the HTTP client can satisfy. This is the
   right factoring but only worth doing once we have measurements
   showing HTTP loopback is in fact a bottleneck.
3. **`sequence_state_checker` for implicit IMPORT mode.**
   `seq_writer.h:26` already exposes a `writes_disabled()` hook
   that can be installed by external code. Hooking the
   replicator task into this would give us "implicit IMPORT"
   semantics: while the link is active, external writes to the
   destination SR are rejected (matching the requirement that
   replicated content is the source of truth). Clean future
   refactor with minimal SR-internal disruption.

## Risks / open issues for the next session

- **Verify the `controller_locked_task` parent runs the singleton
  on shard 0 of the controller leader, not shard 0 of every node.**
  I read `task.cc:250-256` (`is_controller_leader`) and it returns
  true only on the leader, but I have not run this on a multi-node
  setup yet. If it fires on multiple nodes simultaneously, we'll
  get duplicate writes (which are idempotent — same id, same
  version — but wasteful and surface a `not_in_import_mode` race
  if dest mode flips).
- **`extract_config` does a manual copy of the variant.** I
  noticed `schema_registry_sync_config` has no `copy()` helper
  unlike its siblings. A clean follow-up is to add one and use it
  from both the converter and this task to keep the copy logic
  consistent.
- **JSON parsing in `get_subject_version` swallows references
  with malformed shapes.** Tonight it `continue`s when a single
  reference has the wrong type; long-term we probably want to
  fail the whole schema and surface a specific error so users can
  diagnose source-SR-side corruption.
- **Build warnings.** No `-Werror` failures observed but a clean
  pass with `--config=clang-tidy` was not attempted.

## End-to-end test result (2026-05-13 morning)

`tests/rptest/tests/confluent_sr_shadow_link_test.py` — PASS.

```
Topology: Apache Kafka 3.8 (KRaft) + Confluent SR 7.7.1
          + Redpanda single-node destination
Seeded:   3 simple Avro subjects + 1 referent + 1 referrer = 5 schemas
```

Measurements from this run (1-broker dest, in-VM docker):

| signal | value |
|---|---|
| catch-up start → IMPORT mode set | 341 ms |
| catch-up start → 5 schemas + 1 compat replicated | 420 ms |
| new-subject discovery latency (post-catch-up registration → visible on dest) | **258 ms** |
| ID preservation | 5/5 (orders=1, shipments=2, customers=3, common-types=4, events-with-common=5) |
| reference DAG ordering | correct (referrer written after referent) |

Two bugs surfaced + fixed during the e2e bring-up that were
invisible to the unit + gmock tests:

1. The stash that added the Confluent SR ducktape service did not
   wire the corresponding stage into `tests/docker/Dockerfile`, so
   the test-node image was missing `/opt/confluent/bin/`. Fixed by
   adding a `confluent-schema-registry` stage + COPY line.
2. `http::client::request_and_collect_response` does not auto-set
   the `Content-Length` header for non-empty payloads, so the
   destination SR was reading the body as zero bytes and returning
   `422 parse error at offset 0`. `sr_http_client::do_write_json`
   now sets the header explicitly.

## Suggested next steps (in order)

The original "next steps" list collapsed: admin-API surfacing, dest
URL wiring, and the ducktape e2e are all done.  Updated priority
list:

1. Wire the destination URL from `pandaproxy::schema_registry::configuration`
   instead of relying on the `destination_url` override in the
   variant (only the test sets it today). Defaulting to the local
   broker's actual SR port removes a foot-gun for non-default
   configurations.
4. Run that test once with N=20 to validate correctness, then
   N=1000 to get the first set of memory/throughput numbers.
5. Add the gmock-based integration tests for
   `schema_registry_replicator_task` that script HTTP responses
   to drive the catch-up + tail loops; would close the gap
   between "compiles" and "validated."
6. Replicate compat levels (call `get_compatibility`/`put_compatibility`
   from `run_catch_up` and on the slow tail tick).
7. Backoff + rate limiting at the source-client layer.
8. mTLS plumbing (`net::base_transport::configuration::credentials`).
9. Wire the admin-API for per-link status reporting + add per-subject
   status as the E-199 stretch goal.

## Commit history (this branch)

```
$ git log --oneline dev..slsr/poc
28654376af cluster_link/sr: mirror compatibility levels alongside schemas
64487075eb cluster_link/tests: mock-driven HTTP integration tests for sr_http_client
9c58f37da7 docs/plans: findings + handoff notes for overnight SR POC
e0e08ac3a6 admin/shadow_link/converter: handle shadow_via_http_api variant
76bd0adbe3 cluster_link: introduce schema_registry_replicator_task
f4377728fb cluster_link: toposort schemas by references for IMPORT-mode writes
1ef061b7a6 cluster_link/tests: unit tests for sr_http_client pure-logic helpers
544afcc57a cluster_link: add HTTP client wrapper for Schema Registry calls
b134ca98a9 cluster_link/model: add shadow_via_http_api variant for SR sync
cf55eb1f47 docs/plans: record cluster_link survey findings for SR POC
80521885d1 docs/plans: implementation plan for SR shadow link POC
b4af37ec4d docs/plans: design for SR shadow link POC
4dc5b8aa41 tests/rptest: add Confluent SR ducktape harness
```

13 commits, each self-contained and re-orderable; commit-by-commit
review should be feasible. There are no fixup commits because the
issues caught by clang-format / clang were small enough to land in
the same commit. Net diff vs `dev`: ~2.4kLOC of code + tests + docs.

## Final test status

```
$ bazel test //src/v/cluster_link/tests/...
```

13 / 13 PASS, including the three new tests this branch adds:

- `sr_http_client_test` (9 unit tests, ~0.6s) — URL parsing + Basic auth.
- `sr_topo_sort_test` (11 unit tests, ~0.6s) — reference DAG.
- `sr_http_client_mock_test` (9 integration tests, ~0.4s) — gmock-scripted
  HTTP responses cover the full request/response wire protocol.

Wide build is green:

```
$ bazel build //src/v/redpanda //src/v/cluster_link/...
```

i.e. the new variant in `schema_registry_sync_config` does not break
any downstream code, and the new task is fully linked into the
`redpanda` binary.
