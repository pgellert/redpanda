# Shadow Linking Resources

## Knowledge

### Primary source (the code)
- `src/v/cluster_link/model/types.h` — the domain model: every noun and state machine. The reference for all concepts.
- `src/v/cluster_link/link.h` / `link.cc` — per-link coordinator; owns tasks + the replication manager.
- `src/v/cluster_link/task.h` — abstract task base + `controller_locked_task` + `task_factory`. The shape every sync job takes.
- `src/v/cluster_link/replication/partition_replicator.h` / `replication/deps.h` — the data-plane fetch→replicate loop and its `data_source`/`data_sink` abstractions.
- `src/v/cluster_link/topic_reconciler.h`, `link_status_reconciler.h` — control-plane reconcilers (topic creation; failover/promotion).
- `src/v/cluster_link/shadow_linking_rpc.json` — the RPC surface for status reporting.

### Product / behavioural framing (official docs)
- [Shadowing Overview](https://docs.redpanda.com/streaming/current/manage/disaster-recovery/shadowing/overview/)
  Authoritative "what it does and why" — active-passive DR, byte-level offset-preserving replication, what gets mirrored, limitations. Use for: grounding the *purpose* behind a piece of code.
- [Shadow link tasks](https://docs.redpanda.com/streaming/current/manage/disaster-recovery/shadowing/overview/#shadow-link-tasks)
  Maps the three sync tasks (Source Topic Sync, Consumer Group Shadowing, Security Migrator) and their task states. Direct 1:1 with the code's task classes. Use for: confirming task responsibilities.
- [Failover Runbook](https://docs.redpanda.com/streaming/current/manage/disaster-recovery/shadowing/failover-runbook/)
  Operator steps for disaster failover. Use for: understanding the promotion/failover state machine from the outside-in.
- [Shadow Linking lab (Kubernetes)](https://docs.redpanda.com/labs/kubernetes/shadow-linking/)
  Hands-on two-cluster setup. Use for: building intuition by actually running a link end-to-end.

## Wisdom (Communities)
- Internal: the Redpanda Storage/Enterprise team owners of `cluster_link` (per Jira Team = Enterprise/Storage). Use for: design intent, why a state machine looks the way it does.
- Internal Slack: shadow-linking / cluster-linking feature channel. Use for: real-time "why does X behave like Y" questions.

## Gaps
- No in-repo RFC/design doc found for cluster_link (the only RFC is the 2022 raft replication one). The design intent currently lives in code + the docs above + team knowledge. If a design doc surfaces, add it here as the new primary source.
