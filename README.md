# kafka-go — Kafka Broker from Scratch in Go

A broker implemented from scratch in Go with a tested Kafka protocol subset,
on-disk persistence with sparse indexing, and experimental schema validation
with dead-letter queue routing. It is not a production-ready Kafka replacement.


## Supported APIs

| API | Key | Versions | Description |
|-----|-----|----------|-------------|
| **ApiVersions** | 18 | 0–4 | Broker capability discovery & version negotiation |
| **Produce** | 0 | 11 | Uncompressed, nontransactional record batches; acks=0/1/-1 |
| **Fetch** | 1 | 16; legacy 0 | Complete-batch reads within visibility and byte limits; only v16 is advertised |
| **ListOffsets** | 2 | 1 | Earliest/latest offset lookup per partition |
| **Metadata** | 3 | 12 | Broker & topic metadata by name or UUID, including missing-topic errors |
| **OffsetCommit** | 8 | 2 | Durably commit offsets and nullable metadata for a group member |
| **OffsetFetch** | 9 | 1 | Fetch committed offsets and metadata for a consumer group |
| **FindCoordinator** | 10 | 0 | Discover the group coordinator broker |
| **JoinGroup** | 11 | 0 | Join a consumer group (eager rebalance) |
| **Heartbeat** | 12 | 0 | Keep consumer group membership alive |
| **LeaveGroup** | 13 | 0 | Gracefully leave a consumer group |
| **SyncGroup** | 14 | 0 | Distribute partition assignments after rebalance |
| **DescribeTopicPartitions** | 75 | 0 | Detailed partition metadata with partition limits and cursors |

## Architecture

```
app/main.go                  Entry point — config, metadata bootstrap, signal handling
internal/
├── network/
│   ├── server.go            TCP listener with graceful shutdown (context + WaitGroup)
│   ├── connection.go        Per-connection request loop & 13-API dispatch
│   └── admission.go         Shared connection/frame quotas and FIFO backpressure
├── protocol/
│   ├── request.go           Separate size-prefix and admitted frame reads
│   ├── versions.go          Supported versions and shared header/body decoding
│   ├── response.go          Wire-format response framing (v0 / v1 headers)
│   ├── decoder.go           BigEndian binary reader (varints, compact strings, UUIDs)
│   ├── decode_limits.go     Per-request allocation charges and aggregate entry limits
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
│   ├── coordinator.go       Coordinator facade: join/sync/heartbeat/leave
│   ├── lifecycle.go         Cancellation cleanup and tracked timer shutdown
│   └── offsets.go           Checksummed commit log, durable publication, fail-closed replay
├── broker/
│   └── broker.go            Broker registry for simulated multi-broker cluster
├── memory/
│   └── pool.go              Shared FIFO byte reservations with nonblocking resize
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
    ├── record_batch.go      Checked magic-v2 batch codec (CRC32C, offsets, records)
    └── directory.go         Durable directory creation shared by metadata and offsets
```

## Features

- **Binary wire protocol** — Version-specific layouts for the table above, shared nullable client-ID/header handling, bounded decoding, and unknown tagged-field skipping
- **On-disk persistence** — Records written to `.log` files per partition; sparse `.index` files with binary search for offset lookup
- **Checked record batches** — Accepts uncompressed, nontransactional magic-v2 batches; validates CRC32C, record framing, counts, offset deltas, and headers before any append. Compressed and unsupported batch formats return explicit Kafka errors.
- **Record-based offsets** — Assigns base offsets and leader epochs without modifying producer buffers; advances offsets by the number of records across every batch in a request. Index offsets are relative to their segment.
- **Log segment rolling** — Active segment rolls to a new `%020d.log` / `%020d.index` pair when the file exceeds `MaxSegmentBytes` (default 1 GB); `SeekToOffset` selects the correct segment before binary-searching its index
- **Persistent topic metadata** — Atomically saves topic UUIDs, partition replicas, leader IDs/epochs, and relative log paths in `broker-metadata.json`; recovers topics in both supported log layouts, including empty topics
- **Consumer groups** — Full eager-rebalance protocol: FindCoordinator → JoinGroup → SyncGroup → Heartbeat → LeaveGroup with session timeout expiry and automatic rebalance triggers
- **Offset management** — OffsetCommit publishes offsets and nullable metadata only after a complete append/fsync/close; storage failures fence the offset store. Streaming recovery validates checksummed records and preserves valid legacy entries. ListOffsets provides earliest/latest offset discovery.
- **Simulated replication** — Configurable multi-broker cluster (in-process) with round-robin replica assignment, follower fetch simulation, and high watermark advancement
- **LeaderEpoch validation** — Fetch requests carrying a `current_leader_epoch` are fenced: stale epoch → `ErrFencedLeaderEpoch (74)`, epoch newer than the leader's → `ErrUnknownLeaderEpoch (75)`; wrong broker or unassigned replica → `ErrNotLeaderOrFollower (6)`. Epoch `-1` (Fetch v0 / old clients) skips epoch validation.
- **Complete-batch fetches** — Reads across segments using sparse-index seeks (or scanning when an index is absent), retains partition byte budgets, and prevents consumer data above HW from entering a response
- **ISR management** — Removes offline, fenced, timed-out, or excessively lagging replicas; removed followers must fully catch up before rejoining. Membership changes update the partition epoch, recompute HW, and wake pending producers.
- **Produce acks** — Validates `acks=0/1/-1`; `acks=0` sends no response, `acks=1` requires the local append, and `acks=-1` requires the whole append below HW with at least `MIN_INSYNC_REPLICAS` members (default 2). Admission checks and append are atomic under the partition lock.
- **Leader election** — Simple ISR-based leader election; persists the new leader and epoch before publishing them in memory
- **Offset recovery** — Validates every saved replica log, restores record-based LEOs, verifies followers are byte-identical prefixes of the saved leader, and initially derives HW from all assigned replicas. Corrupt, incomplete, noncontiguous, or divergent logs cause startup to fail without truncating data.
- **Concurrency** — Goroutine-per-connection with `sync.RWMutex` on metadata and per-partition mutex on writes
- **Coordinated shutdown** — Cancels listeners, active sockets, replication/group/fetch waits, follower dials, controller probes, and HTTP inference; joins request handlers, workers, and coordinator timer callbacks
- **Request admission** — By default, public listeners share a 256-connection ceiling and 256 MiB raw-frame budget; frames are reserved before allocation, with FIFO admission waits capped at 2 seconds. Replication has separate bounded loopback capacity.
- **AI schema inference** — First message to a topic triggers schema inference via local Ollama (falls back to JSON type detection)
- **Dead-letter queue** — Rejected values are encoded as checksummed Kafka batches in `{topic}.dlq`; a DLQ write failure is reported to the producer. Successful routing remains transparent to the producer.
- **Structured logging** — `slog.Logger` with configurable log level via `LOG_LEVEL` env var

## Independent-client compatibility

The test suite uses **franz-go v1.22.2** and **kmsg v1.15.0** as test-only
dependencies; they are not used to implement the broker. The high-level client
negotiates versions through ApiVersions without overriding the broker's
advertisement. Tests verify:

- Produce, consume, and exact preservation of keys, values, timestamps, and offsets.
- Classic consumer-group discovery, joining, assignment, heartbeats, leaving,
  offset commit/fetch, and resuming from the committed position with a fresh client.
- Independent encoding/decoding of ApiVersions v0-v4 and the v0 error fallback
  for an unsupported newer negotiation version.
- Metadata by name/UUID, explicit errors for unknown topics, and the difference
  between a null topic list (all topics) and an empty list (no topics).
- DescribeTopicPartitions pagination, nullable/long client IDs, unknown tags,
  truncated requests, and malformed collection lengths.

The tested client configuration disables unsupported features:

```go
client, err := kgo.NewClient(
    kgo.SeedBrokers("localhost:9092"),
    kgo.DisableIdempotentWrite(),
    kgo.ProducerBatchCompression(kgo.NoCompression()),
    kgo.DisableFetchSessions(),
    kgo.ConsumerGroup("example-group"),
    kgo.ConsumeTopics("events"),
    kgo.Balancers(kgo.RangeBalancer()),
    kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
)
if err != nil {
    // Handle configuration failure before using client.
    return err
}
defer client.Close()
```

Create topics before using the client; CreateTopics and automatic topic creation
are not implemented. Tests configure one broker with an explicit minimum ISR of
1; the normal runtime default remains 2. Interoperability testing is currently
for one pinned Go client, not a Java/librdkafka/Sarama compatibility certification
or a multi-node fault-tolerance test.

Only advertised versions are accepted, except the documented legacy Fetch v0.
Malformed frames and unsupported non-negotiation API versions are logged and
close the connection rather than being decoded as another version. ApiVersions
uses version-correct arrays and always the non-flexible response header.
Produce responses preserve CreateTime (`log_append_time_ms=-1`), and non-null
arrays are encoded as empty rather than null. Explicit transactional IDs are
rejected. ListOffsets supports earliest/latest sentinels only; timestamp searches
return an explicit unsupported-format error instead of silently returning latest.

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

### Consumer-offset durability and recovery

Consumer offsets live in
`{log.dirs}/__consumer_offsets-0/00000000000000000000.log`, separately from topic
record batches. `LoadOffsets` must succeed before serving offset APIs; the
application and demo initialize it explicitly. An uninitialized coordinator
returns `COORDINATOR_NOT_AVAILABLE` (15), not an in-memory commit success.

For each commit, the coordinator validates the input, member and generation,
holds the coordinator/group locks through the append, then writes, syncs, and
closes the file before publishing the offset in memory. A stale member or
generation cannot change during that validation/write window. Commit metadata
is limited to **4096 UTF-8 bytes**, with `OFFSET_METADATA_TOO_LARGE` (12) on
overflow. Null and empty metadata remain distinct through OffsetFetch and restart.

Open/write/short-write/sync/close failures return `KAFKA_STORAGE_ERROR` (56).
The last published map is not updated, and subsequent offset commits **and
fetches** return a storage error until a new coordinator successfully recovers
the log. A complete but unacknowledged record may be recovered after an I/O
failure, so callers must treat such failures as ambiguous rather than assuming
the commit never happened.

New entries are newline-delimited version-1 JSON envelopes with a CRC32C checksum
over the enclosed commit record. Recovery reads one bounded record at a time
(maximum **256 KiB**, including the newline), validates the entire file, and
publishes only after a successful replay. Invalid JSON, missing fields, invalid
offsets, bad checksums, unknown versions, oversized records, and incomplete tails
stop startup. No entries are skipped; no log is truncated or automatically repaired.
An existing offset directory whose log is missing, an unexpected additional log
segment, or a symlinked offset directory/file is also rejected.

Valid legacy newline-delimited JSON commits remain readable and are never
rewritten during recovery or append. They do not acquire checksum protection
retroactively, and previously discarded commit metadata cannot be recovered.
New and legacy entries can coexist in the same file. Back up damaged logs and
repair them explicitly rather than deleting them to bypass validation.

This is a **local disk durability** guarantee, not replicated coordinator state
or an atomic transaction with consumer processing. Group membership is rebuilt
after restart; offsets are retained independently. Stateless/manual commits
without group membership, offset retention/compaction, and replicated offset
storage are not implemented. The single append log and latest-offset map can
grow without quotas, and commits serialize disk synchronization. Losing the
entire offset directory is indistinguishable from a never-initialized store;
backups and independent failure domains are still required.

### Fetch behavior and limits

The supported fetch layouts are **v16** and legacy **v0**, not the versions
between them. ApiVersions advertises only v16; v0 remains available for consumers
on the public listener and followers on the internal replication listener. Fetch v16 uses the
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

Fetch planning and materialization each hold the partition lock; leadership and
the selected headers are revalidated after waiting for memory capacity. No
partition lock is held during admission waits or network delivery. Index binary
search reads individual entries instead of loading the full index. Record buffers
are sized from the selected batches rather than loading complete segments and
slicing raw bytes; those buffers and the encoded fetch body use the budgets below.

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

### Startup, cancellation, and resource ownership

Startup validates configuration and recovers storage before serving requests.
It binds **all** public and loopback replication listeners before starting accept loops
or background workers. If any bind fails, already-reserved listeners are closed
and startup returns a nonzero exit status. An unexpected accept failure stops the
runtime rather than spinning or leaving a partially running cluster.

Each broker listens on its registered port (`9092 + broker ID - 1`); selecting a
different `BROKER_ID` does not create a second listener on 9092. Integer settings
are validated instead of silently falling back: broker ID must exist, replication
factor must fit the configured broker count, and timing values must be positive.
A missing/unreadable explicitly supplied properties file is a startup error.

On SIGINT/SIGTERM, the runtime immediately:

1. Cancels its root context and closes listeners and active client connections.
2. Releases produce replication waits, fetch long polls, and pending group joins
   and syncs, without waiting for client-controlled timeouts.
3. Cancels follower dial/read/write operations and controller health probes.
4. Stops admission of schema inference, cancels its HTTP requests, and joins the
   inference workers. Inference responses are bounded to 1 MiB.
5. Closes the coordinator, releases pending channels, stops session/rebalance
   timers, and waits for callbacks that were already running.
6. Waits for every listener handler and background worker before returning.

Read and response-write deadlines are both 30 seconds. Shutdown closes sockets
immediately rather than waiting for those deadlines. Ordinary client disconnects
are detected during socket I/O; disconnecting while a handler is waiting is not
an instantaneous cancellation signal. Request-specific timeouts still apply
during normal operation.

This is **not a hard wall-clock shutdown guarantee**: filesystem operations,
recovery scans, or a mutex held by an in-progress disk operation cannot be forcibly
interrupted safely. An append/fsync already in progress is allowed to finish, and
its response may be lost during shutdown. The lifecycle tests verify prompt
cancellation with responsive local storage and separately verify that an
in-progress durable offset commit is not discarded.

For embedded use, `Serve` owns its listener and accepted connections but does
not own a shared coordinator or metadata manager. The caller must handle returned
errors, cancel and join all servers/workers, call `Coordinator.Close()`, and call
`Manager.Schemas.Close()`. Context-aware API variants should be used for cancellable
requests; legacy direct-call wrappers intentionally use a background context.
Worker `Start` methods reject invalid intervals and repeated starts.

### Connection and raw-request admission

The application and demo share **one client admission controller across all
public broker listeners**. The defaults are:

| Limit | Default | Scope |
|-------|---------|-------|
| Accepted client handlers | 256 | Includes idle connections and connections waiting for capacity |
| One request frame | 100 MiB | Header and body, excluding the four-byte size prefix |
| In-flight raw request frames | 256 MiB | Sum of declared sizes reserved by active requests |
| Capacity wait | 2 seconds | FIFO; cancelled promptly during shutdown |

After reading the four-byte size prefix, a handler reserves the entire declared
frame size **before allocating or reading its payload**. The reservation stays
held through parsing, request processing, acknowledgement waits, and response
writes. It is released on every completion/error/cancellation path, including
`acks=0`. Requests are processed sequentially per connection. The wait queue is
also bounded by the connection limit.

Excess connections are closed without starting another handler. Oversized frames
are rejected immediately. Under byte pressure, payload reads pause until a whole
reservation becomes available; the FIFO queue prevents small requests continually
bypassing a large queued request. A capacity timeout closes the connection rather
than inventing a Kafka success/error response before its body has been read.
The existing 30-second frame-read deadline includes time spent waiting for
admission; configured admission waits must not exceed it.

`Admission.Stats()` exposes current connections, reserved bytes, waiter count,
peak usage, and rejection/timeout counters. Every admission rejection is counted;
rejection logs are sampled to at most one per second per controller. These are
in-process counters, not a metrics HTTP endpoint or per-tenant throughput quotas.

#### Protected replication capacity

Each simulated broker also binds an ephemeral **127.0.0.1** replication listener.
Its endpoint is stored in the in-memory broker registry and is never advertised
in public Kafka metadata. Followers use this endpoint exclusively; they do not
fall back to the public port.

The internal listeners share a distinct controller with a **1 MiB raw-frame
budget**, **64 KiB maximum frame**, a **2-second wait**, and enough accepted
connections for one follower-to-leader connection per ordered broker pair
(`max(1, N*(N-1))`; six for the default three brokers). Internal requests contain
one partition, so this smaller frame limit does not reduce supported record-batch
sizes in fetch responses.

Only replica Fetch v0 requests are accepted internally. Public connections cannot
select protected capacity or advance replica state by supplying a replica ID;
such requests are rejected. Conversely, consumer/admin requests are rejected on
the internal listener. Controller health probes still use the public ports.
Tests exhaust client connection, frame, decoder, and fetch budgets while an `acks=-1` produce is pending and verify
that a follower can still replicate and complete that produce.

This separation prevents **client admission pressure** from starving replication.
It is not authentication against other processes on the same host, nor a guarantee
against disk stalls, host-level resource exhaustion, or internal overload.

For embedded multi-listener use, construct one `NewAdmission` instance and pass it
to every public `ServeWithAdmission` call. `Serve` alone creates default limits for
that individual invocation. Use a separate `NewReplicationAdmission` instance for
`ServeReplication`, bind it to loopback, and populate each broker's
`ReplicationAddress` before starting followers. Public and protected controllers
cannot be interchanged.

### Decoder and fetch-buffer budgets

The public admission controller also has independent **128 MiB decoder** and
**512 MiB fetch-buffer** pools. They use the same bounded FIFO wait policy as raw
frames, and their current/peak charges and wait/rejection counters are exposed
through `Admission.Stats().Decoded` and `.Fetch`.

#### Decoded requests

Each wire request has a default **16 MiB allocation-charge ceiling** and
**10,000 aggregate structured entries**. Array elements and produced records
both consume the entry limit, so topic/partition entries reduce the number of
records that fit in a single request. Checks occur before collection growth or
string/byte-field copying. Excess requests are closed before dispatch, and
`DecodeLimitRejections` records these failures.

Charges are deliberately conservative: a 1 KiB base, 256 bytes per entry, and
`2 * length + 64` for each copied string/byte field. Before parsing, the network
layer reserves `min(per-request limit, 1024 + 128 * frame size)`; after successful
parsing it reduces that reservation to the accumulated charge. A charge is not a
Go heap measurement. Null and empty fields retain their distinct wire meanings.

Produce record payloads borrow immutable slices of their already-admitted raw
frame instead of making a second decoder copy. Their record counts are checked
before value descriptors are materialized. Append/fetch/replica validation uses
validation-only batch parsing where values are unnecessary, avoiding per-record
value-slice allocation on those paths.

#### Fetch responses

Normal fetch data is capped at **16 MiB per response**, additionally respecting
client byte limits. Fetch v16 retains its first-batch exception up to the existing
100 MiB batch limit. Legacy v0 still honors the client's partition limit; the
server's soft target may be exceeded by one complete first batch if it fits that
client limit. Replies can contain fewer batches than the byte limit permits:
the selected batch-plan count is also bounded by `MAX_REQUEST_ENTRIES`.

Fetches first inspect headers under the partition lock, without reading payloads.
A planning reservation is held while building this bounded snapshot. Before
materializing records, the reservation is resized to cover planning charges,
response headers, selected record buffers, and the encoded body. If growth would
block, the plan and old reservation are discarded before waiting for a full
reservation; requests never wait for more bytes while retaining a smaller claim.
Empty long polls release planning capacity between polls.

After admission, reads revalidate leadership/epoch and the selected batch headers,
then verify CRCs without creating value slices. Each partition's record buffer and
the final fixed-capacity encoder are sized before allocation. The network writes
the frame header and existing body separately, rather than serializing another
full-size copy. The reservation remains held through response delivery or failure.
Fetch-budget configuration must accommodate the maximum first batch plus encoding
and planning overhead; an impossible configuration fails startup.

Protected replication has its own **8 MiB decoder pool** (1 MiB per request,
1,024 entries), **256 MiB server fetch pool**, and a separate **256 MiB follower
receive pool**. The follower reserves its declared response size plus a bounded
decoder/descriptor allowance before allocation and borrows record bytes during
disk append. Send and receive pools are separate so a sender cannot hold the very
capacity needed by its receiver. Internal fetch data can reach the 100 MiB batch
limit and is not constrained by the public 16 MiB soft target.

**Accounting limit:** these quotas are still not a total-process memory/RSS
ceiling. They account for nominal buffer capacities and decoder/planning charges,
not allocator rounding or garbage awaiting collection. Retained consumer-group,
offset and schema state, JSON schema validation/inference allocations, append
copies, non-fetch response builders, filesystem/directory metadata, socket buffers,
listener backlog, and OS file-descriptor headroom need additional limits and
load-based sizing. Direct API helpers enforce per-call limits; aggregate accounting
requires the shared network admission controller and holding returned fetch payload
reservations until the caller finishes using them.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `info` | Logging verbosity: `debug`, `info`, `warn`, `error` |
| `BROKER_ID` | `1` | This broker's ID in the cluster |
| `NUM_BROKERS` | `3` | Number of simulated brokers in the cluster |
| `REPLICATION_FACTOR` | `3` | Replica count for new topics; must not exceed configured brokers |
| `MIN_INSYNC_REPLICAS` | `2` | Minimum ISR for `acks=-1`; explicitly use `1` for single-copy acknowledgements |
| `REPLICA_LAG_TIME_MAX_MS` | `10000` | Positive lag timeout for ISR maintenance |
| `MAX_CONNECTIONS` | `256` | Shared accepted-client-handler ceiling across public listeners |
| `MAX_REQUEST_BYTES` | `104857600` | Per-frame cap, from 8 bytes to the implementation maximum of 100 MiB |
| `MAX_INFLIGHT_REQUEST_BYTES` | `268435456` | Shared raw-frame byte budget; must be at least `MAX_REQUEST_BYTES` |
| `ADMISSION_WAIT_MS` | `2000` | Request-capacity wait, from 1 to 30000 ms |
| `MAX_DECODED_REQUEST_BYTES` | `16777216` | Per-request decoder-charge ceiling; 1 KiB to 100 MiB |
| `MAX_INFLIGHT_DECODE_BYTES` | `134217728` | Shared decoder-charge budget; at least the per-request ceiling |
| `MAX_REQUEST_ENTRIES` | `10000` | Aggregate request-entry limit and maximum selected fetch batch plans; 1 to 100000 |
| `FETCH_MAX_BYTES` | `16777216` | Public fetch-data soft target; 1 byte to 100 MiB, with first-batch progress exception |
| `MAX_INFLIGHT_FETCH_BYTES` | `536870912` | Shared fetch planning/record/encoding budget; validated for maximum-batch progress |
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

# Independent-client interoperability (no external Kafka or Ollama needed)
go test -race ./internal/network -run 'TestFranzGo|TestIndependentCodec' -v
go test ./internal/protocol -run='^$' -fuzz=FuzzRequestDispatch -fuzztime=30s

# Offset disk-failure injection and independent-client broker-process restart
go test -race ./internal/coordinator ./internal/api/offset_commit ./internal/api/offset_fetch ./internal/network ./app
go test ./internal/coordinator -run='^$' -fuzz=FuzzOffsetLogReplay -fuzztime=30s

# Cancellation, timer cleanup, listener failure, and startup/shutdown regressions
go test -race ./app ./internal/network ./internal/coordinator ./internal/metadata ./internal/replication ./internal/controller ./internal/schema

# Exact admission ceilings, FIFO/cancellation, allocation order, and protected replication
go test -race ./internal/network ./internal/protocol ./internal/replication ./app

# Decoder allocation checks, fetch-buffer admission, growth and streaming
go test -race ./internal/memory ./internal/protocol ./internal/storage ./internal/api/fetch ./internal/network
```

### Test Coverage

| Package | Tests |
|---------|-------|
| `protocol` | Shared nullable/long headers, independent-codec requests, truncation/trailing-data checks, version gating, frame boundaries, decoder bounds, dispatch fuzzing |
| `schema` | Validation, JSON inference, integer↔number coercion, cancellation/joining of an in-flight HTTP inference request |
| `storage` | CRC32C and record validation, independent wire fixture, multiple batches, null values, headers, malformed input, bounded recovery reads, parser fuzzing |
| `metadata` | Record offsets/indexes, corruption preservation, persistent topic/leader recovery across processes, legacy import, divergent/missing replicas, malformed catalogs, atomic DLQ creation, persistence-failure fencing |
| `coordinator` | Group lifecycle and cancelled/superseded joins/syncs; tracked timer close; stale expiry callbacks; offset I/O faults, replay, generation locking, and finishing disk sync during shutdown |
| `replication` | TCP follower persistence and subsequent HW acknowledgement, batches exceeding 1 MiB, failed-write fencing, ISR shrink/expand |
| `api/api_versions` | All 13 API keys present, error propagation |
| `api/produce` | Invalid inputs without side effects, minimum ISR, shared request deadline, whole-append waits, DLQ acknowledgement and persistence-error propagation |
| `app` | Corrupt storage/config rejection, partial-bind cleanup, all-broker cancellation, fatal listener errors, and non-default primary broker ports |
| `api/fetch` | Exact record/offset assertions, HW boundaries, first-batch exception, partition/global byte limits, segment/index fallbacks, offset/leader errors, long polls, concurrent append/fetch |
| `network` | TCP and independent-client flows; lifecycle cleanup; exact connection/frame quotas, FIFO/cancellation races, all-path reservation release, and replication progress under exhausted client capacity |

## Limitations & Future Work

- **In-process replication** — Multi-broker is simulated within a single process; no real network replication between separate broker instances
- **Remaining correctness work** — Persisted replication commit/ISR state, replicated coordinator storage, failover/divergence recovery, remaining shared-state races outside the protected paths, and load/fault testing are still outstanding; the local topic catalog is not distributed consensus
- **Resource limits** — Connection, raw-frame, wire-decoder and fetch-buffer admission are implemented; retained-state quotas, non-fetch response budgets, JSON-processing limits and retention remain. These limits do not bound total RSS, and cancellation cannot guarantee a fixed exit deadline on stalled filesystems.
- **Compatibility scope** — Only the listed versions and configured client subset are supported. More API versions, independent client implementations, timestamp offset lookup, static/cooperative groups, and administrative APIs require additional work.
- **Batch subset** — Compression, transactional/control batches, and compacted logs with noncontiguous record offsets are not supported
- **No cooperative rebalance** — Only eager (stop-the-world) rebalance protocol is supported
- **Partial DLQ filtering** — When some (not all) records in a batch fail schema validation, the full batch still writes to the main topic (individual record filtering from binary RecordBatch is not supported)
- **No authentication** — No SASL or TLS
- **No transactions** — No transactional produce or exactly-once semantics
