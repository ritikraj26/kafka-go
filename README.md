# kafka-go — Kafka Broker from Scratch in Go

A Kafka-compatible broker implemented from scratch in Go, supporting the binary wire protocol, on-disk persistence with sparse indexing, and AI-powered schema validation with dead-letter queue routing.


## Supported APIs

| API | Key | Versions | Description |
|-----|-----|----------|-------------|
| **ApiVersions** | 18 | 0–4 | Broker capability discovery & version negotiation |
| **Produce** | 0 | 0–11 | Write record batches (supports acks=0/1/-1) |
| **Fetch** | 1 | 0–16 | Read record batches up to high watermark |
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
│   ├── topic.go             Topic/Partition: append, sparse index, HW, LEO, replicas
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
- **Cluster metadata recovery** — Parses `__cluster_metadata` KIP-631 records on startup to recover topic UUIDs
- **Consumer groups** — Full eager-rebalance protocol: FindCoordinator → JoinGroup → SyncGroup → Heartbeat → LeaveGroup with session timeout expiry and automatic rebalance triggers
- **Offset management** — OffsetCommit/OffsetFetch for consumer group offset tracking; ListOffsets for earliest/latest offset discovery
- **Simulated replication** — Configurable multi-broker cluster (in-process) with round-robin replica assignment, follower fetch simulation, and high watermark advancement
- **LeaderEpoch validation** — Fetch requests carrying a `current_leader_epoch` are fenced: stale epoch → `ErrFencedLeaderEpoch (74)`, epoch newer than the leader's → `ErrNotLeaderOrFollower (6)`; epoch `-1` (Fetch v0 / old clients) skips validation
- **ISR management** — Automatic ISR shrink when a replica exceeds the lag timeout **or** falls more than 10,000 messages behind the leader LEO (dual check); ISR expand when the replica catches up
- **Produce acks** — `acks=-1` waits for the end offset of the entire append and returns `REQUEST_TIMED_OUT` when that boundary is not reached. Replica-progress correctness and the network behavior of `acks=0` still need hardening.
- **Leader election** — Simple ISR-based leader election with epoch tracking
- **Offset recovery** — Validates every segment of discovered partition logs, restores record-based LEOs, and derives HW from recovered ISR replica offsets. Corrupt, incomplete, or noncontiguous logs cause startup to fail without truncating data.
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

The current loader discovers topics from the legacy `{log.dirs}/{topic}-{partition}`
layout. It also validates logs inside `broker-N` directories and recovers known
replica offsets, but does **not** yet reconstruct topics stored exclusively in
that layout or persist new topic UUIDs/leader metadata. These are separate
metadata-recovery tasks; this step does not establish full cluster restart safety.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `info` | Logging verbosity: `debug`, `info`, `warn`, `error` |
| `BROKER_ID` | `1` | This broker's ID in the cluster |
| `NUM_BROKERS` | `3` | Number of simulated brokers in the cluster |
| `REPLICATION_FACTOR` | `3` | Default replication factor for new topics |
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
```

### Test Coverage

| Package | Tests |
|---------|-------|
| `protocol` | Encoder/decoder roundtrip, varint encoding, compact strings, UUID, all 13 request parsers, `CurrentLeaderEpoch` field in Fetch v16, v0 epoch defaults to -1 |
| `schema` | Validation (missing field, wrong type, not JSON), inference from JSON, integer↔number coercion |
| `storage` | CRC32C and record validation, independent wire fixture, multiple batches, null values, headers, malformed input, bounded recovery reads, parser fuzzing |
| `metadata` | Multi-record offsets, unchanged input buffers, relative indexes beyond INT32 offsets, segment rolling, recovery of every segment, corruption preservation, replica validation, append fencing, HW timeouts |
| `coordinator` | Single/multi-member join, sync group, heartbeat, leave + rebalance, offset commit/fetch |
| `replication` | Follower HW advancement, ISR shrink on time lag, ISR shrink on LEO lag (>10 000 messages), ISR expand on catch-up |
| `api/api_versions` | All 13 API keys present, error propagation |
| `api/produce` | Invalid-batch error codes and no write side effects, multi-batch offsets, whole-append acknowledgement target, valid DLQ batches and persistence-error propagation |
| `app` | Subprocess test proving damaged logs cause nonzero startup exit and remain unchanged |
| `api/fetch` | Unknown topic ID error, known topic with records, `ErrFencedLeaderEpoch` (stale epoch), `ErrNotLeaderOrFollower` (epoch too new) |
| `network` | End-to-end ApiVersions roundtrip, full consumer group lifecycle integration test |

## Limitations & Future Work

- **In-process replication** — Multi-broker is simulated within a single process; no real network replication between separate broker instances
- **Remaining correctness work** — Full topic/cluster metadata recovery, durable follower acknowledgements, fetch visibility/batch boundaries, API-version compatibility, and runtime data races are not resolved by record-batch validation
- **Batch subset** — Compression, transactional/control batches, and compacted logs with noncontiguous record offsets are not supported
- **No cooperative rebalance** — Only eager (stop-the-world) rebalance protocol is supported
- **Partial DLQ filtering** — When some (not all) records in a batch fail schema validation, the full batch still writes to the main topic (individual record filtering from binary RecordBatch is not supported)
- **No authentication** — No SASL or TLS
- **No transactions** — No transactional produce or exactly-once semantics
