# Cluster Link (Shadow Linking) - Claude Code Guide

## Overview

The `cluster_link` module implements Redpanda's **Cluster Linking** (also called "Shadow Linking" or "Panda Linking") feature for Enterprise Disaster Recovery. It enables asynchronous data mirroring from a **source cluster** to a **destination cluster** using a **pull-based architecture**.

**Key Insight:** The destination cluster drives everything - it fetches data from the source cluster via the Kafka protocol, preserving records byte-for-byte with identical offsets and timestamps.

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          DESTINATION CLUSTER                                 │
│                                                                              │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐                  │
│  │   service   │─────▶│   manager   │─────▶│    link     │                  │
│  │ (API layer) │      │ (lifecycle) │      │ (per-link)  │                  │
│  └─────────────┘      └─────────────┘      └──────┬──────┘                  │
│         │                    │                    │                          │
│         │                    │           ┌───────┴────────┐                 │
│         │                    │           │                │                 │
│         │                    │     ┌─────▼─────┐    ┌─────▼─────┐          │
│         │                    │     │   tasks   │    │replication│          │
│         │                    │     │(periodic) │    │  manager  │          │
│         │                    │     └───────────┘    └─────┬─────┘          │
│         │                    │           │                │                 │
│         ▼                    ▼           ▼                ▼                 │
│  ┌─────────────────────────────────────────────────────────────────┐       │
│  │                    link_registry (controller table)              │       │
│  └─────────────────────────────────────────────────────────────────┘       │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                            Kafka Fetch Protocol
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                           SOURCE CLUSTER                                     │
│                       (Regular Redpanda/Kafka)                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
src/v/cluster_link/
├── service.{h,cc}              # API layer - user-facing operations
├── manager.{h,cc}              # Manages link lifecycle, handles notifications
├── link.{h,cc}                 # Individual link instance with tasks
├── link_probe.{h,cc}           # Metrics collection per link
├── task.{h,cc}                 # Base class for periodic tasks
├── deps.{h,cc}                 # Dependency injection interfaces
├── errc.{h,cc}                 # Error codes and error_info wrapper
├── fwd.h                       # Forward declarations
├── types.h                     # ntp_leader bool class
├── logger.{h,cc}               # Logging utilities
│
├── model/                      # Data model types
│   ├── types.{h,cc}            # Core types: metadata, configs, states
│   └── filter_utils.{h,cc}     # Topic name filtering
│
├── replication/                # Data replication subsystem
│   ├── link_replication_mgr.{h,cc}   # Per-link replicator lifecycle
│   ├── partition_replicator.{h,cc}   # Per-partition fetch+replicate loop
│   ├── mux_remote_consumer.{h,cc}    # Multiplexed Kafka consumer
│   ├── partition_data_queue.{h,cc}   # Buffer between fetch and replicate
│   ├── deps.{h,cc}             # data_source/data_sink interfaces
│   ├── types.{h,cc}            # Replication-specific types
│   └── replication_probe.{h,cc}# Replication metrics
│
├── utils/                      # Utilities
│   └── topic_properties_utils.{h,cc} # Topic property sync logic
│
├── source_topic_syncer.{h,cc}  # Task: discovers & syncs source topics
├── group_mirroring_task.{h,cc} # Task: mirrors consumer group offsets
├── security_migrator.{h,cc}    # Task: syncs ACLs from source
├── topic_reconciler.{h,cc}     # Creates/updates local mirror topics
├── link_status_reconciler.{h,cc} # Reconciles link state transitions
│
├── rpc_service.{h,cc}          # RPC handlers for cross-node comms
├── shadow_linking_rpc.json     # RPC protocol definition
│
└── tests/                      # Unit tests
    ├── deps.{h,cc}             # Test mocks/fixtures
    └── *_test.cc               # Individual test files
```

## Core Components

### 1. `service` (service.h:41)
The **API layer** - a sharded service that handles user requests:
- `upsert_cluster_link()` - Create or update a link
- `delete_cluster_link()` - Remove a link
- `update_mirror_topic_status()` - Pause/resume/failover topics
- `failover_link_topics()` - Failover all topics on a link
- `shadow_link_report()` / `shadow_topic_report()` - Get status

Owns a single `manager` instance per shard.

### 2. `manager` (manager.h:38)
**Manages link lifecycle** and handles system notifications:
- Creates/destroys `link` instances based on controller table changes
- Forwards leadership change notifications to appropriate links
- Owns task factories that create tasks for each link
- Runs periodic task reconciliation

Key callbacks:
- `on_link_change()` - Controller table updated
- `handle_partition_state_change()` - Partition leadership changed

### 3. `link` (link.h:32)
**Represents a single cluster link** - the connection to one source cluster:
- Owns a `kafka::client::cluster` connection to source
- Owns `replication::link_replication_manager` for data replication
- Manages registered `task` instances
- Handles leadership changes for mirror partitions

### 4. `task` (task.h:33)
**Abstract base class for periodic tasks** that run on a link:
- `start()` / `stop()` / `pause()` - Lifecycle management
- `run_impl()` - Subclasses implement periodic work
- State machine: `stopped` → `active` ↔ `paused` ↔ `link_unavailable` → `faulted`

Built-in task types:
- `controller_locked_task` - Only runs on controller leader node

### 5. Dependency Interfaces (deps.h)
**Abstract interfaces for testability**:
- `link_registry` - Access to controller's cluster link table
- `link_factory` - Creates `link` instances
- `cluster_factory` - Creates Kafka client connections
- `consumer_groups_router` - Consumer group operations
- `security_service` - ACL creation

## Replication Subsystem

### `link_replication_manager` (replication/link_replication_mgr.h:26)
**Manages partition replicators** for a link on one shard:
- `start_replicator(ntp, term)` - Start replication for a partition
- `stop_replicator(ntp, term)` - Stop replication
- Uses reconciliation pattern to handle concurrent start/stop requests

### `partition_replicator` (replication/partition_replicator.h:57)
**Per-partition fetch-and-replicate loop**:
```
┌─────────────┐         ┌─────────────────────┐         ┌───────────┐
│ data_source │────────▶│ partition_replicator │────────▶│ data_sink │
│  (fetch)    │         │   (coordinates)      │         │(replicate)│
└─────────────┘         └─────────────────────┘         └───────────┘
```
- Pipelines up to 5 concurrent replicate requests
- Handles backoff on failures
- Tracks offsets for lag reporting

### `mux_remote_consumer` (replication/mux_remote_consumer.h:42)
**Multiplexed Kafka consumer** for efficient multi-partition fetching:
- Single consumer manages many partitions
- Per-partition data queues prevent slow partitions from blocking others
- Supports dynamic partition add/remove

### Data Flow Interfaces (replication/deps.h)
- `data_source` - Fetches batches from source (wraps mux_remote_consumer)
- `data_sink` - Replicates batches to local partition (wraps write_at_offset_stm)

## Tasks (Periodic Jobs)

### `source_topic_syncer` (source_topic_syncer.h:29)
**Discovers and syncs topics from source cluster**:
1. Queries source cluster metadata
2. Filters topics by configured patterns
3. Fetches topic configurations via DescribeConfigs
4. Submits `add_mirror_topic_cmd` to controller

Runs on: **Controller leader only**

### `group_mirroring_task` (group_mirroring_task.h:44)
**Mirrors consumer group offsets**:
1. Lists consumer groups from source
2. Fetches committed offsets
3. Commits offsets to local __consumer_offsets
4. Clamps offsets to partition HWM to avoid invalid offsets

Runs on: **Shards owning __consumer_offsets partitions**

### `security_migrator` (security_migrator.h:27)
**Syncs ACLs from source cluster**:
1. Calls DescribeACLs on source
2. Creates equivalent ACLs locally

Runs on: **Controller leader only**

## Data Model (model/types.h)

### Key Types
```cpp
using id_t = named_type<int64_t, ...>;    // Internal link ID
using name_t = named_type<ss::sstring, ...>; // User-visible link name
```

### `mirror_topic_status` State Machine
```
                     ┌─────────────┐
                     │   PAUSED    │
                     └─────────────┘
                           ▲ │
                           │ ▼
                     ┌─────────────┐
        ┌────────────│   ACTIVE    │────────────┐
        │            └─────────────┘            │
        ▼                  │                    ▼
  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
  │FAILING_OVER │───▶│   FAILED    │◀───│ PROMOTING   │
  └─────────────┘    └─────────────┘    └─────────────┘
        │                                       │
        ▼                                       ▼
  ┌─────────────┐                      ┌─────────────┐
  │ FAILED_OVER │                      │  PROMOTED   │
  └─────────────┘                      └─────────────┘
```

### `task_state` States
- `active` - Task running normally
- `paused` - User paused the task
- `link_unavailable` - Transient connection/auth issue
- `stopped` - Task not configured to run
- `faulted` - Unexpected error

### Configuration Types
- `connection_config` - Bootstrap servers, TLS, SASL credentials
- `topic_metadata_mirroring_config` - Topic sync settings
- `consumer_groups_mirroring_config` - Consumer offset sync settings
- `security_settings_sync_config` - ACL sync settings

## Error Handling (errc.h)

```cpp
enum class errc {
    success = 0,
    invalid_task_state_change,
    link_id_not_found,
    link_connection_failed,
    topic_already_mirrored,
    license_required,
    // ... see errc.h for full list
};

// Wraps errc with optional message
class err_info {
    errc _ec;
    std::string _msg;
};

template<typename T>
using cl_result = result<T, err_info>;
```

## Testing Strategy

Tests use **dependency injection** via abstract interfaces in `deps.h`:
- Mock `link_registry` to control what links exist
- Mock `kafka::client::cluster` to simulate source cluster responses
- Mock `data_source`/`data_sink` to test replication logic in isolation

Key test files:
- `link_test.cc` - Link lifecycle and task management
- `source_topic_syncer_test.cc` - Topic discovery logic
- `group_mirroring_task_test.cc` - Consumer offset mirroring
- `partition_replicator_fixture_tests.cc` - Replication loop

## Common Patterns

### 1. Reconciliation Pattern
Used in `link_replication_manager` and `topic_reconciler`:
```cpp
// Target state is updated
_pending[ntp].set_desired(new_state);
// Background loop reconciles
ss::future<> reconcile() {
    for (auto& [ntp, state] : _pending) {
        if (state.needs_reconciliation()) {
            state.in_progress = state.desired;
            co_await do_reconcile(ntp);
            state.in_progress = std::nullopt;
        }
    }
}
```

### 2. Work Queue Pattern
Used in `service` and `manager` to serialize state changes:
```cpp
ssx::work_queue _queue;
void on_notification() {
    _queue.submit([this] { return handle_notification(); });
}
```

### 3. Controller-Locked Tasks
Tasks that must run only on controller leader:
```cpp
class my_task : public controller_locked_task {
    // Automatically starts/stops based on controller leadership
};
```

## Metrics

Exposed via `link_probe`:
- `shadow_link_*` - Per-link metrics (topic counts by state)
- `partition_replicator_*` - Per-partition lag, bytes fetched/written

## Key Files to Understand First

1. **Start here:** `service.h` - Entry point, shows available operations
2. **Then:** `manager.h` - How links are created/destroyed
3. **Then:** `link.h` - What a link does
4. **Then:** `task.h` - How periodic work is structured
5. **For replication:** `replication/partition_replicator.h` - The core loop

## Common Tasks

### Adding a New Task Type
1. Create class inheriting from `task` or `controller_locked_task`
2. Implement `run_impl()`, `update_config()`, `is_enabled()`
3. Create a `task_factory` subclass
4. Register factory in `manager::start()` via `register_task_factory<>()`

### Debugging Replication Issues
1. Check `partition_replicator` logs for fetch/replicate errors
2. Look at `mux_remote_consumer` for connection issues
3. Check `data_sink` for write_at_offset_stm errors

### Understanding Topic State Transitions
See `model::is_valid_status_transition()` in `model/types.cc` for allowed transitions.
