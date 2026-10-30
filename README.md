# kafka-go — Kafka Broker from Scratch in Go

A Kafka-compatible broker implemented from scratch in Go, supporting the binary wire protocol, on-disk persistence with sparse indexing, and AI-powered schema validation with dead-letter queue routing.


## Supported APIs

| API | Key | Versions | Description |
|-----|-----|----------|-------------|
| **ApiVersions** | 18 | 0–4 | Broker capability discovery & version negotiation |
| **Produce** | 0 | 0–11 | Write record batches (supports acks=0/1/-1) |
| **Fetch** | 1 | 16; legacy 0 | Complete-batch reads within visibility and byte limits; only v16 is advertised |
| **ListOffsets** | 2 | 1 | Earliest/latest offset lookup per partition |
| **Metadata** | 3 | 0–12 | Broker & topic metadata (UUID, partitions, ISR) |
| **OffsetCommit** | 8 | 2 | Commit consumer offsets to coordinator |
| **OffsetFetch** | 9 | 1 | Fetch committed offsets for a consumer group |
| **FindCoordinator** | 10 | 0 | Discover the group coordinator broker |
| **JoinGroup** | 11 | 0 | Join a consumer group (eager rebalance) |
| **Heartbeat** | 12 | 0 | Keep consumer group membership alive |
| **LeaveGroup** | 13 | 0 | Gracefully leave a consumer group |
| **SyncGroup** | 14 | 0 | Distribute partition assignments after rebalance |
| **DescribeTopicPartitions** | 75 | 0 | Detailed partition metadata per topic |

## Architecture

```
app/main.go                  Entry point — config, metadata bootstrap, signal handling
internal/
├── network/
│   ├── server.go            TCP listener with graceful shutdown (context + WaitGroup)
│   └── connection.go        Per-connection request loop & 13-API dispatch
├── protocol/
│   ├── request.go           Wire-format request header (with maxRequestSize guard)
│   ├── response.go          Wire-format response framing (v0 / v1 headers)
│   ├── decoder.go           BigEndian binary reader (varints, compact strings, UUIDs)
│   ├── encoder.go           BigEndian binary writer (v0 non-flexible + compact formats)
│   ├── request_types.go     Typed request structs + parsers for all 13 APIs
│   ├── consumer_protocol.go MemberMetadata / MemberAssignment binary codec
│   ├── api_keys.go          API key constants (13 APIs)
│   └── error_codes.go       Kafka error code constants
├── api/
│   ├── api_versions/        ApiVersions response builder
│   ├── produce/             Produce response + acks handling + schema validation + DLQ
│   ├── fetch/               Fetch response (reads up to high watermark)
│   ├── metadata/            Metadata v12 response (multi-broker aware)
│   ├── describe_topics/     DescribeTopicPartitions response builder
│   ├── find_coordinator/    FindCoordinator — returns group coordinator broker
│   ├── join_group/          JoinGroup — blocking join with eager rebalance
│   ├── sync_group/          SyncGroup — leader distributes partition assignments
│   ├── heartbeat/           Heartbeat — session keepalive
│   ├── leave_group/         LeaveGroup — graceful group departure
│   ├── offset_commit/       OffsetCommit — persist consumer offsets
│   ├── offset_fetch/        OffsetFetch — retrieve committed offsets
│   └── list_offsets/        ListOffsets — earliest/latest offset lookup
├── coordinator/
│   ├── group.go             Consumer group state machine (Empty→Joining→AwaitingSync→Stable)
│   └── coordinator.go       Coordinator facade: join/sync/heartbeat/leave + offset store
├── broker/
│   └── broker.go            Broker registry for simulated multi-broker cluster
├── metadata/
│   ├── metadata.go          Thread-safe topic/partition manager (RWMutex)
│   ├── persistence.go       Atomic topic catalog (IDs, replicas, paths, leader epochs)
│   ├── topic.go             Topic/Partition: append, sparse index, HW, LEO, replicas
│   ├── fetch.go             Locked, bounded cross-segment reads and replica progress
│   ├── produce.go           Atomic produce admission, ISR policy, acknowledgement waits
│   ├── leader_election.go   Simple leader election within ISR
│   └── loader.go            Disk recovery: log scanning, cluster metadata parsing
├── replication/
│   ├── follower.go          Simulated follower fetch — advances replica LEOs & HW
│   └── isr.go               ISR management: shrink on lag, expand on catch-up
├── schema/
│   ├── registry.go          Thread-safe schema registry + JSON validation
│   └── infer.go             Schema inference (Ollama LLM → local JSON fallback)
└── storage/
    └── record_batch.go      Checked magic-v2 batch codec (CRC32C, offsets, records)
```

## Features

- **Binary wire protocol** — Full Kafka protocol serialization: varints, compact arrays, compact strings, UUIDs, TAG_BUFFERs
- **On-disk persistence** — Records written to `.log` files per partition; sparse `.index` files with binary search for offset lookup
- **Checked record batches** — Accepts uncompressed, nontransactional magic-v2 batches; validates CRC32C, record framing, counts, offset deltas, and headers before any append. Compressed and unsupported batch formats return explicit Kafka errors.
- **Record-based offsets** — Assigns base offsets and leader epochs without modifying producer buffers; advances offsets by the number of records across every batch in a request. Index offsets are relative to their segment.
- **Log segment rolling** — Active segment rolls to a new `%020d.log` / `%020d.index` pair when the file exceeds `MaxSegmentBytes` (default 1 GB); `SeekToOffset` selects the correct segment before binary-searching its index
- **Persistent topic metadata** — Atomically saves topic UUIDs, partition replicas, leader IDs/epochs, and relative log paths in `broker-metadata.json`; recovers topics in both supported log layouts, including empty topics
- **Consumer groups** — Full eager-rebalance protocol: FindCoordinator → JoinGroup → SyncGroup → Heartbeat → LeaveGroup with session timeout expiry and automatic rebalance triggers
- **Offset management** — OffsetCommit/OffsetFetch for consumer group offset tracking; ListOffsets for earliest/latest offset discovery
- **Simulated replication** — Configurable multi-broker cluster (in-process) with round-robin replica assignment, follower fetch simulation, and high watermark advancement
- **LeaderEpoch validation** — Fetch requests carrying a `current_leader_epoch` are fenced: stale epoch → `ErrFencedLeaderEpoch (74)`, epoch newer than the leader's → `ErrUnknownLeaderEpoch (75)`; wrong broker or unassigned replica → `ErrNotLeaderOrFollower (6)`. Epoch `-1` (Fetch v0 / old clients) skips epoch validation.
- **Complete-batch fetches** — Reads across segments using sparse-index seeks (or scanning when an index is absent), retains partition byte budgets, and prevents consumer data above HW from entering a response
- **ISR management** — Removes offline, fenced, timed-out, or excessively lagging replicas; removed followers must fully catch up before rejoining. Membership changes update the partition epoch, recompute HW, and wake pending producers.
- **Produce acks** — Validates `acks=0/1/-1`; `acks=0` sends no response, `acks=1` requires the local append, and `acks=-1` requires the whole append below HW with at least `MIN_INSYNC_REPLICAS` members (default 2). Admission checks and append are atomic under the partition lock.
- **Leader election** — Simple ISR-based leader election; persists the new leader and epoch before publishing them in memory
- **Offset recovery** — Validates every saved replica log, restores record-based LEOs, verifies followers are byte-identical prefixes of the saved leader, and initially derives HW from all assigned replicas. Corrupt, incomplete, noncontiguous, or divergent logs cause startup to fail without truncating data.
- **Concurrency** — Goroutine-per-connection with `sync.RWMutex` on metadata and per-partition mutex on writes
- **Graceful shutdown** — `SIGINT`/`SIGTERM` handling via `signal.NotifyContext`; listener close + `WaitGroup` drain
- **Size guards** — 100 MB maximum request frame and record batch size; aggregate connection/memory limits remain future work
- **AI schema inference** — First message to a topic triggers schema inference via local Ollama (falls back to JSON type detection)
- **Dead-letter queue** — Rejected values are encoded as checksummed Kafka batches in `{topic}.dlq`; a DLQ write failure is reported to the producer. Successful routing remains transparent to the producer.
- **Structured logging** — `slog.Logger` with configurable log level via `LOG_LEVEL` env var

## Running

```sh
# Start the broker (default: port 9092, log dir /tmp/kraft-broker-logs)
go run ./app/main.go

# With a custom config file
go run ./app/main.go /path/to/server.properties

# Explicitly opt into one-copy acknowledgements for a single-broker run
NUM_BROKERS=1 REPLICATION_FACTOR=1 MIN_INSYNC_REPLICAS=1 go run ./app/main.go
```

The broker reads `log.dirs` from the properties file to locate partition logs on disk.

### Recovery policy and compatibility

Partition log recovery is **fail-closed**: invalid checksums, incomplete batches,
unsupported formats, and offset discontinuities stop startup. Log files are
preserved for inspection; no automatic truncation or repair is performed.
An append I/O failure also fences further leader appends to that partition until
restart and successful recovery, because the failed write may have changed disk.

Older versions of the demo and seed tools wrote zero CRCs, and the old append
path did not assign valid per-record offsets. Those logs may now be rejected.
Back up existing data before investigating or explicitly repairing it. Do not
run the seed script over data you want to preserve: it overwrites its seed files.

### Persistent topic catalog

The broker stores a versioned `broker-metadata.json` in `log.dirs`. Updates use a
same-directory temporary file, file synchronization, atomic rename, and directory
synchronization. Topic creation initializes empty replica logs before publishing
the catalog, so an empty topic also survives restart. Creation and leader-election
errors are returned to their callers; an ambiguous metadata I/O failure fences
further metadata changes until restart. A failed leader change also fences writes
to that partition. Partial creation can leave untracked directories; recovery
reports them instead of silently adopting them.

The catalog preserves topic IDs, partition counts, ordered replica assignments,
leader IDs/epochs, and relative log paths. New topics use
`broker-{id}/{topic}-{partition}`. Replica placement sorts broker IDs and honors
`REPLICATION_FACTOR`; changing that default does not reassign existing topics.
Topic names use Kafka's ASCII letters/digits/`.`/`_`/`-` subset (maximum 249 bytes);
reserved internal names are rejected. The current safety limits are 10,000
partitions per topic and a 16 MiB catalog.

On the first startup without a catalog, the loader can import a partition with
exactly one discovered owner from either a flat `{topic}-{partition}` directory
(broker 1) or a `broker-N` directory. Existing UUIDs from a valid legacy
`__cluster_metadata` topic-record log are retained; otherwise an ID is assigned
once and saved. Partition gaps and multiple possible owners are rejected rather
than guessing a leader. Import does not add replicas based on today's default RF.
Old multi-replica directories without authoritative metadata therefore require
explicit operator recovery. Do not delete the catalog to bypass validation.

Startup fails if a saved directory or its entire segment set is missing, a
configured broker is missing from the registry, an untracked partition is found,
or a replica is ahead of/diverges from the saved leader. Malformed catalogs and
unsafe saved paths are rejected. Temporary metadata files are never promoted
automatically. Logs are not repaired or truncated by recovery.

ISR membership and replica heartbeat timestamps are runtime state, not durable
consensus state. Startup initially includes all assigned replicas in ISR and
calculates a conservative HW from their verified on-disk ends; a lagging replica
may delay visibility until catch-up. This is not a persisted commit watermark,
and does not fix the outstanding runtime replication/ISR issues.

Use only one active broker process/manager per log directory. The catalog is for
this in-process simulator, not a multi-process controller quorum or a distributed
metadata service. Schemas and consumer-group membership are not persisted here;
consumer offsets continue to use their separate log.

### Fetch behavior and limits

The supported fetch layouts are **v16** and legacy **v0**, not the versions
between them. ApiVersions advertises only v16; v0 remains available for the
internal follower and explicitly configured legacy callers. Fetch v16 uses the
standard nullable client ID, validates/skips tagged fields, and defaults to
consumer mode unless the ReplicaState tag identifies a follower.

Consumer reads include only complete batches whose exclusive end offset is at
or below the partition's HW snapshot. Replica reads may go up to leader LEO.
If the requested consumer offset falls inside a batch, the complete containing
batch is returned, including earlier records in that batch; clients must discard
records before their requested offset. Replica offsets must be batch boundaries.
Requests before log start or beyond LEO return `OFFSET_OUT_OF_RANGE`; requests at
LEO (or at/above HW for consumers) return no records.

Fetch v16 honors both whole-request and per-partition byte budgets. As in Kafka's
modern fetch behavior, the first nonempty batch may exceed either budget, even
a zero budget, to allow progress. That exception applies only once per response;
later batches must fit. Legacy v0 keeps a strict per-partition limit and never
returns a partial batch: a caller whose limit is smaller than the next batch must
raise its limit.

The internal v0 follower requests one partition at a time with a limit large
enough for any supported batch. It updates its local position only after a
successful append/fsync. The leader uses the next request's fetch offset as
reported prior progress rather than assuming the response has already reached
disk. Stale responses after an election are rejected; failed follower writes
fence further appends to that replica until recovery.

Fetch validates leadership, offsets and visibility while holding the partition
lock through the read. Index binary search reads individual entries instead of
loading the full index, and record reads allocate at most one validated batch
at a time in addition to the response. Reads stop at their byte/visibility
boundary rather than loading a complete segment and slicing raw bytes. This
favors correctness over concurrent append throughput; there is no global
connection/memory quota yet.

Long polling still uses short polling intervals. `min_bytes=0` and partition
errors return immediately. Incremental sessions are not implemented: responses
use session ID 0, and requests for a nonzero session return
`FETCH_SESSION_ID_NOT_FOUND`. Transactional isolation is not implemented (the
write path rejects transactional/control batches); follower-read routing and
divergent-log truncation are also outside the supported subset.

### Producer acknowledgement policy

`MIN_INSYNC_REPLICAS` defaults to **2**, including when existing topic definitions
are loaded. It is a broker-wide startup policy, not persisted per-topic metadata.
Invalid values (including an explicitly empty value) fail startup rather than
silently falling back. A topic with fewer assigned replicas may still be read
or written using `acks=0/1`, but cannot satisfy `acks=-1` until the policy or
replica assignment is explicitly changed. The single-broker example above opts
into the weaker minimum of 1.

| Request/outcome | Behavior |
|-----------------|----------|
| `acks=0` | Append locally without waiting for replicas and send no response. A partition failure is logged and closes the connection without response bytes. |
| `acks=1` | Respond after the local append/fsync; the minimum-ISR policy does not apply. |
| `acks=-1` | Check minimum ISR before append, then wait for the exclusive end offset of the entire append to reach HW. |
| Any other `acks` value | `INVALID_REQUIRED_ACKS` (21), without source or DLQ writes. |
| Negative `timeout_ms` | `INVALID_REQUEST` (42), without writes. |
| Too few ISR members before append | `NOT_ENOUGH_REPLICAS` (19), without appending. |
| Minimum lost while waiting | `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (20); the local append remains. |
| Replication wait expires | `REQUEST_TIMED_OUT` (7); the local append may already exist. |
| Leader/epoch changes while waiting | `NOT_LEADER_OR_FOLLOWER` (6), without claiming successful replication. |

The minimum is an admission floor, **not a quorum target**: with ISR `[1,2,3]`
and minimum 2, `acks=-1` waits for all three until ISR legitimately shrinks.
Shrinking to two caught-up members can complete the wait; shrinking below two
fails pending acknowledgements even if ISR recovers before the waiter runs.
Controller-marked offline brokers cannot be re-added by a maintenance tick just
because they fetched recently. Reachability alone is not sufficient to rejoin.

All partitions and DLQ writes in one Produce request share one replication-wait
deadline. This does not cancel an in-progress filesystem write or undo an append;
later partitions can still append after the wait budget expires. DLQ routing
uses the original acknowledgement setting and minimum-ISR policy rather than
silently returning success after a leader-only write. The source and DLQ writes
are not an atomic transaction.

After a timeout, connection loss, or post-append error, a retry may duplicate
records. There is no idempotent-producer or exactly-once guarantee. Acknowledged
runtime replication is also not a substitute for persisted commit/ISR state,
independent failure domains, safe divergent-log recovery, or broker authentication.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `info` | Logging verbosity: `debug`, `info`, `warn`, `error` |
| `BROKER_ID` | `1` | This broker's ID in the cluster |
| `NUM_BROKERS` | `3` | Number of simulated brokers in the cluster |
| `REPLICATION_FACTOR` | `3` | Replica count for new topics; must not exceed configured brokers |
| `MIN_INSYNC_REPLICAS` | `2` | Minimum ISR for `acks=-1`; explicitly use `1` for single-copy acknowledgements |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama API endpoint for schema inference |
| `OLLAMA_MODEL` | `llama3.2` | LLM model for schema inference |

## Testing

```sh
# Run the full test suite
go test ./...

# Run with verbose output
go test ./... -v

# Run only the integration test (consumer group lifecycle over TCP)
go test ./internal/network/ -run Integration_ConsumerGroup -v

# Run only coordinator unit tests
go test ./internal/coordinator/ -v

# Run replication & ISR tests
go test ./internal/replication/ -v

# Run batch, offset, recovery, produce, and startup regressions
go test ./internal/storage ./internal/metadata ./internal/api/produce ./app

# Check concurrent execution and fuzz the record-batch parser
go test -race ./...
go test ./internal/storage -run='^$' -fuzz=FuzzParseRecordBatches -fuzztime=30s

# Fetch visibility, byte budgets, follower progress, and malformed request parsing
go test -race ./internal/api/fetch ./internal/metadata ./internal/replication ./internal/network ./internal/protocol
go test ./internal/protocol -run='^$' -fuzz=FuzzFetchRequests -fuzztime=30s

# Acknowledgement, ISR-transition, configuration, and wire-level failure tests
go test -race ./internal/api/produce ./internal/metadata ./internal/replication ./internal/network ./app
```

### Test Coverage

| Package | Tests |
|---------|-------|
| `protocol` | Encoder/decoder roundtrip, varint encoding, compact strings, UUID, all 13 request parsers, `CurrentLeaderEpoch` field in Fetch v16, v0 epoch defaults to -1 |
| `schema` | Validation (missing field, wrong type, not JSON), inference from JSON, integer↔number coercion |
| `storage` | CRC32C and record validation, independent wire fixture, multiple batches, null values, headers, malformed input, bounded recovery reads, parser fuzzing |
| `metadata` | Record offsets/indexes, corruption preservation, persistent topic/leader recovery across processes, legacy import, divergent/missing replicas, malformed catalogs, atomic DLQ creation, persistence-failure fencing |
| `coordinator` | Single/multi-member join, sync group, heartbeat, leave + rebalance, offset commit/fetch |
| `replication` | TCP follower persistence and subsequent HW acknowledgement, batches exceeding 1 MiB, failed-write fencing, ISR shrink/expand |
| `api/api_versions` | All 13 API keys present, error propagation |
| `api/produce` | Invalid inputs without side effects, minimum ISR, shared request deadline, whole-append waits, DLQ acknowledgement and persistence-error propagation |
| `app` | Startup rejection of corrupt storage and invalid minimum ISR; explicit single-copy configuration |
| `api/fetch` | Exact record/offset assertions, HW boundaries, first-batch exception, partition/global byte limits, segment/index fallbacks, offset/leader errors, long polls, concurrent append/fetch |
| `network` | TCP produce/restart/fetch, Fetch v16 visibility, acks=0 suppression and failure closure, ISR-loss error codes, acks=-1 with real follower disk persistence |

## Limitations & Future Work

- **In-process replication** — Multi-broker is simulated within a single process; no real network replication between separate broker instances
- **Remaining correctness work** — Persisted commit/ISR state, failover/divergence recovery, other API-version compatibility, and concurrency/lifecycle issues outside the locked produce/fetch paths are still outstanding; the local topic catalog is not distributed consensus
- **Batch subset** — Compression, transactional/control batches, and compacted logs with noncontiguous record offsets are not supported
- **No cooperative rebalance** — Only eager (stop-the-world) rebalance protocol is supported
- **Partial DLQ filtering** — When some (not all) records in a batch fail schema validation, the full batch still writes to the main topic (individual record filtering from binary RecordBatch is not supported)
- **No authentication** — No SASL or TLS
- **No transactions** — No transactional produce or exactly-once semantics
