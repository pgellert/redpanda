# Mission: Shadow Linking (`src/v/cluster_link/`)

## Why
Gellert is becoming a code owner / reviewer of the `cluster_link` subsystem. He needs a durable mental model of the *whole* subsystem — strong enough to review PRs, reason about failure modes, and place any new change correctly — not just a tour of one file. He already works adjacent to it (schema registry replication on `slsr/schema-replication`).

## Success looks like
- Given any file in `src/v/cluster_link/`, he can say which architectural plane and pipeline it belongs to, and why.
- He can narrate the end-to-end lifecycle of a shadow link from creation → topic discovery → data replication → status reporting, naming the responsible types.
- He can reason about failover/promotion state transitions and where split-brain risks live.
- He can connect the cluster-link replication model to schema-registry replication.

## Constraints
- Expert C++/Seastar/raft/Kafka-protocol background — do NOT teach those primitives; teach the shadow-linking architecture built on top of them.
- Time-boxed: short lessons, one durable win each.
- Lessons should cite real `file:line` references he can click and verify.

## Out of scope (for now)
- Seastar/coroutine mechanics, raft internals, Kafka wire protocol basics.
- `rpk`/CLI UX and Kubernetes operator (`ShadowLink` CRD) except as context.
- Writing new cluster_link features — this is about understanding first.
