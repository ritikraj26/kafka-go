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
- **ISR management** — Automatic ISR shrink when a replica exceeds the lag timeout **or** falls more than 10,000 messages behind the leader LEO (dual check); ISR expand when the replica catches up
- **Produce acks** — `acks=-1` waits for the end offset of the entire append and returns `REQUEST_TIMED_OUT` when that boundary is not reached. Follower positions advance after disk synchronization, not when the leader constructs a response. ISR/minimum-replica policy, crash durability, and the network behavior of `acks=0` still need hardening.
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

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `info` | Logging verbosity: `debug`, `info`, `warn`, `error` |
| `BROKER_ID` | `1` | This broker's ID in the cluster |
| `NUM_BROKERS` | `3` | Number of simulated brokers in the cluster |
| `REPLICATION_FACTOR` | `3` | Replica count for new topics; must not exceed configured brokers |
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
| `api/produce` | Invalid-batch error codes and no write side effects, multi-batch offsets, whole-append acknowledgement target, valid DLQ batches and persistence-error propagation |
| `app` | Subprocess test proving damaged logs cause nonzero startup exit and remain unchanged |
| `api/fetch` | Exact record/offset assertions, HW boundaries, first-batch exception, partition/global byte limits, segment/index fallbacks, offset/leader errors, long polls, concurrent append/fetch |
| `network` | ApiVersions roundtrip, consumer group lifecycle, TCP produce/restart/fetch, Fetch v16 consumer visibility without replica-state mutation |

## Limitations & Future Work

- **In-process replication** — Multi-broker is simulated within a single process; no real network replication between separate broker instances
- **Remaining correctness work** — Persisted commit/ISR state, minimum-ISR policy, failover/divergence recovery, other API-version compatibility, and concurrency/lifecycle issues outside the locked fetch path are still outstanding; the local topic catalog is not distributed consensus
- **Batch subset** — Compression, transactional/control batches, and compacted logs with noncontiguous record offsets are not supported
- **No cooperative rebalance** — Only eager (stop-the-world) rebalance protocol is supported
- **Partial DLQ filtering** — When some (not all) records in a batch fail schema validation, the full batch still writes to the main topic (individual record filtering from binary RecordBatch is not supported)
- **No authentication** — No SASL or TLS
- **No transactions** — No transactional produce or exactly-once semantics
