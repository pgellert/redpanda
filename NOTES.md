# Teaching Notes — Shadow Linking

## Learner profile
- Redpanda engineer, expert in C++/Seastar/raft/Kafka protocol. Do NOT teach those primitives.
- Works on schema-registry replication (`slsr/schema-replication`), adjacent to cluster_link.
- Mission = **code ownership** of `cluster_link/`: place any file, narrate the lifecycle, reason about failover.

## Preferences
- Lessons cite clickable `file:line` refs; verify line numbers before asserting.
- Keep lessons short, one durable win each. High ZPD — he can take dense material.
- Ground "why" claims in official docs (Shadowing DR pages) when possible.

## Curriculum plan (tentative)
- [x] 0001 — Two planes (control/data) + tasks + reporting. The skeleton.
- [x] 0002 — Where work runs: should_start_impl predicate; controller_locked_task vs broker-local; unifying "reconcile work to leadership" pattern (incl. partition_replicator). LR-0001 recorded.
- [x] 0003 — Control-plane hand-off: task → frontend → controller raft → table::apply_update (every node) → topic_reconciler (leader only) → topic_creator. Desired-state/reconcile pattern. Naming trap: src/v/cluster_link (engine) vs src/v/cluster/cluster_link (controller state: frontend+table). link_registry = DI seam.
- [x] 0004 — Data plane: fetch_and_replicate loop (Kafka fetch from source → local raft append), 5-deep pipeline semaphore. Offset preservation = sink passes explicit expected_offsets (source base_offset) to raft. Three guards: monotonicity (step down/recover), producer-ID sync (post-failover idempotency), prefix-truncate (source retention realign). Read-only shadow b/c local write would mint offset.
- [x] 0005 — Failover & promotion: mirror_topic_status state machine (types.h:111, transitions types.cc:35); failover (immediate, source gone) vs promotion (drains lag first); link_status_reconciler::try_finish_failover uses report RPC + link-revision heuristic; identity NOT preserved (topic ID: source vs destination_topic_id separate; leader epoch: committed -1) — framed as open debate, code facts only; split-brain = process not protocol. Reference: failover-state-machine.html.
- [x] 0006 — Schema Registry sync (CAPSTONE): topic mode (byte-replicate _schemas via data plane) vs API mode (schema_registry_mirroring_task: source_reader → import via write_source::schema_registry_sync). Write gate matrix client×mode (frontend.cc:336-344): client blocked both modes; sync blocked in topic mode (data plane owns _schemas), allowed in API mode. Task leads _schemas/0 (locality, LR-0001). Maps every prior pattern onto his own feature. PR0 scope = default context only.

## MISSION COMPLETE (2026-06-16)
Full arc 0001–0006 delivered. User owns the cluster_link lifecycle end-to-end. Future work = PR1+ increments of his SR-sync feature (contexts/filtering/references/compat/deletes/unsupported-feature handling); offer review/lessons as those land.
Verified correction this session: the Kafka produce write-gate for mirror topics is is_topic_mutable (kafka/data/rpc/service.cc:298 → frontend.cc:240 → switch :86-98): mutable only for failed_over/promoted; blocked for active/failing_over/promoting/paused/failed. Unblock is emergent from replicated status reaching failed_over, NOT a per-partition toggle. (A sub-agent reported this inverted — verified directly.)

## Workspace location
- Teaching workspace now lives in worktree `/home/gellertnagy/code/redpanda-teach-shadow-linking` (branch `teach/shadow-linking`). Author future lessons HERE.
- `file:line` refs verified against the `slsr/schema-replication` working tree (main checkout, with feature mods). Line numbers are approximate in this worktree's committed code; conceptually identical.

## Reference docs
- reference/architecture-map.html — four buckets, naming map.
- reference/task-state-machine.html — task lifecycle (live vs dormant states) + rpk operator controls.
- reference/failover-state-machine.html — mirror_topic_status transitions, failover vs promote, identity preservation.

## Known facts (verified)
- No `rpk shadow pause/resume` subcommand. Pause = `rpk shadow update <LINK>` + set `paused: true` per task section (types.go:146/187/199). CLI `paused` ⇔ core `is_enabled = !paused` (mapper.go:227/262/282).
- Task states split live (active/link_unavailable/faulted — runner retries on timer) vs dormant (paused/stopped — only reconciler should_start revives, and only from those two states). link_unavailable self-heals.

## Open threads
- Failover identity memory: topic ID + leader epoch NOT preserved across failover (active debate). Good material for 0005.
