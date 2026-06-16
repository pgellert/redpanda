# Task locality follows the locality of the state it writes

Gellert derived, unprompted, the principle that a cluster_link task runs wherever the state it writes lives: partitioned state (consumer offsets in `__consumer_offsets`) → run at each partition leader (`group_mirroring_task`, a plain `task`); controller-global state (mirror-topic registrations, ACLs) → single writer on the controller leader (`source_topic_syncer`, `security_migrator`, both `controller_locked_task`). He also correctly intuited that this is co-location to avoid cross-broker write RPCs, not a separate RPC path.

**Evidence:** Answered the Lesson 0001 §4 cliffhanger with the locality argument, including the correct hedge about whether cross-broker commit RPCs exist (they don't — the group coordinator *is* the partition leader; `group_mirroring_task.cc:34-48`, `task.cc:260-267`).

**Implications:** He doesn't need "where does state live in Redpanda" taught — he reaches for controller-raft-vs-partition-raft naturally. Lesson 0002 can go straight to the *mechanism* (`should_start_impl`/`should_stop_impl` predicates, the task reconciler) rather than motivating why locality matters. Sets the altitude high for the control-plane deep dive (0003). Connects to [[MISSION.md]] failover reasoning.
