# Distributed Priority Queue Service 

**Status: design finalized, implementation in progress.** This document is the
full design record — every decision, the reasoning behind it, and the
tradeoffs that were rejected. It's written to be sufficient on its own for a
human or an agent to implement the system, and to be defended live in a
follow-up walkthrough where the design will be extended and questioned.

## 1. Overview

This service is the **Queue Service** described in the assignment: it owns
named queues, message lifecycle (enqueue → dequeue/lease → acknowledge →
retry → dead-letter), and operational metrics. Producers and consumers are
just callers of its API — this repo does not implement them beyond thin
test/demo harnesses.

Required operations: `Create Queue`, `Enqueue`, `Dequeue`, `Acknowledge`,
`Get Metrics`. Priority is `HIGH` / `MEDIUM` / `LOW` with FIFO ordering
within a tier. Delivery is at-least-once via lease + visibility timeout,
with a configurable redelivery threshold before a message is dead-lettered.

## 2. Architecture

Two layers, deliberately separated:

- **Core engine** (`com.dpqs.core`) — framework-free. No HTTP framework, no
  DI container, no serialization library. This is where every concurrency,
  data-structure, and lifecycle decision lives, and it's the part meant to
  be evaluated on its own merits. It depends on Micrometer's `MeterRegistry`
  interface only — recording a counter/gauge is not "a framework," it's a
  metrics facade, and Micrometer is the locked stack choice for metrics.
- **Adapter** (`com.dpqs.service`) — a thin Javalin HTTP layer translating
  requests to core-engine calls and serializing responses. Javalin was
  chosen over Spring Boot for the existing walking-skeleton for the same
  reason it holds here: an embedded Jetty server with no
  classpath-scanning/autoconfiguration magic, sub-second startup, and it
  stays out of the way of the core engine's own threading model. It also
  gives `Get Metrics` a real scrapeable HTTP endpoint, which the spec
  requires ("queryable... by a monitoring system like Prometheus").

The boundary rule: if you could unit-test it without starting an HTTP
server, it belongs in the core engine.

## 3. Design decisions at a glance

| # | Decision | Choice | Key reason |
|---|----------|--------|------------|
| 1 | Priority structure | Three FIFO queues (one per tier), not a `PriorityBlockingQueue` | PBQ serializes all tiers on one internal lock; three queues let HIGH enqueue and LOW dequeue proceed independently |
| 2 | Concurrency primitive | Lock-free per-tier queues + counting semaphore for blocking dequeue | Avoids reintroducing a global lock; explicit locks would be simpler but block producers/consumers against each other unnecessarily |
| 3 | Fairness | Strict priority for v1; selection policy is a swappable layer | Spec's "should usually" hedges on this; keep a 20-minute path to add aging without a rearchitecture |
| 4 | Timer model | One `DelayQueue` per queue + dedicated **virtual thread** reaper per queue | `DelayQueue.take()` blocks via `Lock`/`Condition`, which doesn't pin a virtual thread's carrier — so per-queue isolation is cheap on Java 21 |
| 5 | Lease renewal / heartbeat | Out of scope for v1 (documented gap) | No renew op in the spec's API surface; a long-running consumer can be redelivered mid-processing — known limitation |
| 6 | Ack vs. timer entry | Tombstone: ack only writes state + WAL, never touches the timer | Decouples the ack hot path from the scheduler entirely; the timer fires later regardless and no-ops against terminal state |
| 7 | Message state transitions | CAS-based (`AtomicReference<State>` / `ConcurrentHashMap.compute`), fenced by `redeliveryCount` as a lease token | Lower contention than a per-message lock; ordering rule below prevents the ack-vs-expiry race; reusing `redeliveryCount` as a fencing token stops a late ack from a superseded lease from deleting a message a *different* consumer is now actively holding |
| 8 | Backpressure | Bounded per-tier queues, reject immediately when full; capacity tracked by a dedicated eager counter, decoupled from `readyPermits` | Unbounded queues risk OOM under producer/consumer imbalance; blocking-with-timeout deferred as a v2 refinement; decoupling avoids a TTL-expired-but-not-yet-popped message holding a capacity slot it no longer needs |
| 9 | Durability | File-based WAL, additive to the in-memory core | "File" is spec-sanctioned; keeps 100% of the concurrency design in-memory while making the durability NFR real, not just prose |
| 10 | WAL granularity | One WAL file per queue | A shared file would reintroduce a global bottleneck across unrelated queues, undoing decision #2 at the durability layer |
| 11 | WAL fsync policy | Synchronous — fsync before returning success to the caller | Strongest guarantee; explicit tradeoff against p95 latency, accepted for v1 and revisited only if load testing shows the NFR isn't met — batching is the named follow-up, not built preemptively |
| 12 | WAL write ordering | Log-before-apply | Standard WAL discipline; a crash between log and apply is always safely recoverable |
| 13 | Horizontal scaling | Shard by queue name; one instance owns a queue at a time (narrative) | Matches an in-memory, framework-free core; avoids requiring external infra |
| 14 | Node failure handling | Narrative only — WAL log-shipping to a standby | Honest answer given a single-instance, in-memory implementation |
| 15 | Adapter transport | Embedded HTTP via Javalin | Matches existing skeleton; gives metrics a real scrape endpoint |
| 16 | DLQ visibility | Internal/test-only accessor, no public list/replay API | Spec only asks for a DLQ *count* via metrics |
| 17 | Config defaults | Concrete defaults on `Create Queue` (30s visibility timeout, 5 retries, 10,000 capacity/tier) | Caller doesn't have to specify everything to get a usable queue |
| 18 | Testing | Hand-rolled multithreaded stress harness + deterministic `Clock` injection, not jcstress | Faster to write and fully explainable live; WAL crash-recovery tests added as a required category |
| 19 | Dequeue blocking semantics (HTTP) | Bounded long-poll: `tryAcquire(waitMs)` on `readyPermits`, empty result (not an error) on timeout, capped server-side wait | Indefinite blocking on an HTTP call risks client/load-balancer timeout mismatches; matches SQS's `ReceiveMessage` long-poll model |

## 4. Core data model

```
QueueManager
  └── ConcurrentHashMap<String queueName, QueueInstance>   (created by Create Queue, dynamically, at runtime)

QueueInstance (per queue)
  ├── QueueConfig                    (visibility timeout, retry limit, per-tier capacity)
  ├── TierQueue[3]                   (HIGH, MEDIUM, LOW — one lock-free FIFO queue each)
  ├── AtomicInteger[3] tierCapacityUsed  (eager occupancy count per tier; gates Enqueue backpressure — decoupled from readyPermits, see §5)
  ├── Semaphore readyPermits         (total ready messages across all tiers; gates blocking dequeue)
  ├── ConcurrentHashMap<messageId, MessageRecord>   (source of truth for message state)
  ├── DelayQueue<TimerEntry> + 1 virtual-thread reaper   (lease + TTL expiry)
  ├── Dlq                            (terminal store for messages past the retry threshold)
  ├── WalWriter                      (this queue's own append-only log file)
  └── QueueMetrics                   (per-tier counters, in-flight counter, DLQ counter, oldest-age tracking)
```

`MessageRecord` holds: `messageId` (opaque, e.g. UUID — deliberately *not*
derived from any internal ordering sequence), `payload`, `priority`,
`enqueuedAt` (via injected `Clock`, not `System.currentTimeMillis()`
directly — needed for deterministic tests and for the TTL/oldest-age
calculations), `ttlDeadline` (nullable), `state`, `redeliveryCount`.

**Note on sequence numbers:** the original design sketch assumed an
`AtomicLong` sequence for FIFO tie-breaking, needed because a
`PriorityBlockingQueue`'s heap has no inherent insertion order within equal
priority. Once tiers became three separate FIFO queues (decision #1), this
is no longer needed for ordering — a `ConcurrentLinkedQueue`'s own insertion
order *is* the FIFO-within-tier guarantee. A per-queue sequence number may
still be useful internally as a WAL record index (to detect gaps/corruption
on replay), but it plays no role in priority ordering.

### 3.1 Message lifecycle / state machine

```
READY ──dequeue──▶ IN_FLIGHT ──ack──────────────▶ ACKED (terminal, removed from map)
                       │
                       ├──lease expiry, count < threshold──▶ READY (redeliveryCount++)
                       │
                       └──lease expiry, count ≥ threshold──▶ DEAD (moved to DLQ, terminal)

READY ──TTL expiry──▶ DEAD (or dropped, per config — terminal, never dequeued)
```

Legal transitions are enforced by CAS on `MessageRecord.state`
(`AtomicReference<State>` or `ConcurrentHashMap.compute`), not by external
locking. **Ordering rule that prevents the ack-vs-expiry race:** the WAL
write for a transition must complete and be durable *before* that
transition is considered committed and visible to other threads
(log-before-apply). If the reaper's expiry-triggered transition and a
consumer's ack race for the same message at the same instant, whichever
CAS succeeds first wins; the loser detects its CAS failed, re-reads the
now-current state, and no-ops rather than overwriting it.

**Lease fencing (prevents a late ack from deleting someone else's active
lease):** `state == IN_FLIGHT` alone is not sufficient to authorize an ack
— it only tells you *a* lease is active, not *whose*. A consumer whose
lease already expired and was redelivered (state cycled `IN_FLIGHT →
READY → IN_FLIGHT` under a new owner) would otherwise be able to ack
against the new owner's active lease, since both look identical from state
alone — this matters because the service can never verify that a late ack
actually corresponds to real completion of the *current* lease rather than
a stale/zombie signal from a superseded one. `Dequeue` returns the
message's current `redeliveryCount` alongside its ID; `Acknowledge`
requires the caller to pass it back (`ack(messageId, expectedRedeliveryCount)`),
and the CAS only commits `IN_FLIGHT → ACKED` if `state == IN_FLIGHT &&
redeliveryCount == expected`. `redeliveryCount` already increments exactly
once per lease handoff (on expiry-triggered redelivery, never on the
original dequeue) and is already WAL-logged as `REDELIVERY_INCREMENT`, so
it doubles as a lease generation token with no new field or extra
durability work required. A late ack carrying a stale `redeliveryCount`
fails the CAS and no-ops rather than erroneously deleting a message
someone else currently holds; a genuine, on-time ack always matches.

## 5. Priority & concurrency model

Three independent, lock-free FIFO queues per `QueueInstance` — one per
tier — instead of a single `PriorityBlockingQueue`. Rationale: PBQ's heap
is guarded by one internal lock, so *every* enqueue and dequeue across
*every* tier serializes on it regardless of which tier they actually touch.
Three queues let a HIGH producer and a LOW consumer proceed fully in
parallel. It also avoids PBQ's O(n) removal-by-key, which the design has no
current need for but would be a dead end if ever required (e.g. cancel a
specific message).

Dequeue selection policy: scan HIGH → MEDIUM → LOW, return the first
non-empty tier's head. This is intentionally isolated as its own component
(not inlined into the queue structures) so that **fairness/aging can be
added later — e.g. promoting a LOW message after a wait threshold — without
touching the tier-queue or concurrency design.** v1 ships strict priority
only; this is a known, deliberate scope cut given the spec's "should
usually receive the highest-priority" wording leaves room for both
interpretations.

Blocking dequeue (when all tiers are empty) is implemented with a single
`Semaphore` per queue, whose permit count tracks total ready messages
across all three tiers: `release()` after any enqueue or expiry-triggered
requeue, `acquire()` (blocking) before a dequeue attempt. This avoids
explicit locks on the hot path entirely — no `synchronized`, no
`ReentrantLock`. The tradeoff accepted here: correctness of the permit
count vs. real queue occupancy has to be maintained carefully (every path
that adds a message to a tier queue must release exactly one permit, no
more, no less) — the rejected alternative (lock + `Condition`) is easier to
reason about but reintroduces serialization between producers and
consumers.

**TTL expiry of a never-dequeued message, and permit reconciliation.** A
message whose TTL fires while it's still sitting `READY` in its tier queue
was never popped by a consumer, so nothing removes its physical node —
searching a `ConcurrentLinkedQueue` for it would reintroduce the O(n)
removal-by-key this design deliberately avoided (see the PBQ discussion
above). The reaper instead CASes `MessageRecord.state: READY → DEAD` and
leaves the node in place as a tombstone — the same pattern as the ack
tombstone (decision #6), applied to the queue structure instead of the
timer. `Dequeue`'s pop loop is therefore: `acquire()` a permit, `poll()`
the highest-priority tier's head, and if that record's state is no longer
`READY` (already tombstoned `DEAD`), discard it and loop — `acquire()`
again for the next attempt. Each discard consumes exactly the permit that
was issued for that now-dead message at enqueue time, so the count
self-reconciles with no separate bookkeeping. This is also why the Oldest
Message Age gauge (§11) needs a *tombstone-aware* peek rather than a plain
`peek()` — it's reading the same tombstoned heads `Dequeue` would discard.

**Why this can't be the same counter used for backpressure (§8).** A
tombstoned node still physically occupies a slot in its bounded tier queue
until some future `Dequeue` call happens to pop past it — which may not
happen soon on a tier with light consumer traffic. If `Enqueue`'s capacity
check used `readyPermits` (or tier queue size) directly, a tier full of
TTL-expired tombstones would spuriously reject new, legitimate enqueues
even though the "real" backlog is empty. So capacity accounting is a
separate, eagerly-maintained `AtomicInteger` per tier (`tierCapacityUsed`,
§4): incremented on enqueue, decremented immediately on *any* terminal
transition (`ACK`, `DLQ_MOVE`, TTL-`DEAD`) regardless of whether the
physical node has been popped yet. `readyPermits` answers "is there a
message to hand a consumer" and reconciles lazily; `tierCapacityUsed`
answers "is there room for a new one" and must be exact immediately — they
are deliberately two different counters with two different consistency
requirements.

**Blocking `Dequeue` over HTTP.** An HTTP call blocking indefinitely on
`acquire()` risks mismatched client/load-balancer timeouts and pins a
connection for an unbounded time. The adapter instead uses
`tryAcquire(waitMs, TimeUnit.MILLISECONDS)` — a bounded long-poll,
mirroring SQS's `ReceiveMessage` `WaitTimeSeconds`. The caller supplies an
optional `waitMs` (default a few seconds, capped server-side at ~30s); on
timeout the endpoint returns 200 with an empty result, not an error — an
empty poll is a normal outcome, not a failure. Longer waits are the
client's responsibility via repeated calls, not a longer server-side
block.

## 6. Timers, leases & TTL expiry

One `DelayQueue<TimerEntry>` per `QueueInstance`, serviced by one dedicated
**virtual thread** running `while (running) { entry = delayQueue.take();
handle(entry); }`. `TimerEntry` carries `(deadline, messageId, reason)`
where `reason` is `LEASE_EXPIRY` or `TTL_EXPIRY` — these require different
actions (redeliver + increment vs. terminal death) and must not be
conflated at fire time.

**Why per-queue instead of one shared scheduler across all queues:** the
classic objection to "one thread per queue" is OS thread cost at scale.
`DelayQueue.take()` blocks via `ReentrantLock`/`Condition`, which — unlike
a `synchronized` block — does not pin a virtual thread's carrier thread. On
Java 21, running each queue's reaper as `Thread.ofVirtual().start(...)`
makes per-queue isolation cheap even with hundreds of queues, so the
simplicity and blast-radius isolation of "one DelayQueue per queue" is kept
without paying the platform-thread cost that would otherwise argue for a
shared `ScheduledExecutorService`.

**Ack vs. timer entry (decision #6):** ack does **not** cancel or remove
the corresponding `TimerEntry`. It only writes the ACK record to this
queue's WAL and flips the message's state to `ACKED`. The timer entry is
left to fire on schedule; when it does, the reaper checks current state,
sees `ACKED`, and no-ops. This was chosen over active removal because it
decouples the ack hot path completely from the timer subsystem — no
coordination, no cancel-vs-concurrently-firing race to reason about — at
the cost of some wasted wake-ups for messages that ack quickly relative to
their visibility timeout.

**Lease renewal / heartbeat: explicitly out of scope for v1.** The spec's
API surface has no renew/extend operation. Consequence, stated plainly: a
consumer whose processing legitimately exceeds the visibility timeout will
have its message redelivered while still working on it. This is the first
extension point to reach for if asked to improve delivery guarantees.

## 7. Durability — write-ahead log

Additive to the in-memory core, not a replacement for it. Every
concurrency-critical structure (tier queues, state map, timers) stays
exactly as designed above; the WAL only makes selected state transitions
durable across a process restart.

**One append-only file per queue** (`wal/<queueName>.log`), not one shared
file for the instance. A shared file would force every queue's durable
writes to serialize on the same file descriptor — reintroducing, at the
durability layer, exactly the global bottleneck decision #2 was designed to
avoid at the data-structure layer. The cost of per-queue files (N open file
handles, no single global cross-queue ordering) is minor at this scale and
is the same tradeoff already accepted for per-queue timer threads.

**Events logged:** `CREATE_QUEUE` (name + resolved config — this is
effectively the header record of a queue's own file), `ENQUEUE` (payload,
priority, TTL, timestamp, messageId), `ACK`, `DLQ_MOVE`, and a
`REDELIVERY_INCREMENT` record (written specifically when a lease-timeout
triggers a requeue, not on every dequeue).

**Deliberately not logged:** lease/dequeue (`IN_FLIGHT`) state, and TTL
expiry events. Rationale: delivery is already at-least-once, so a message
that was `IN_FLIGHT` at crash time can simply be treated as `READY` on
replay — worst case is one extra redelivery, which is already within the
delivery contract. TTL expiry needs no separate event either: the TTL
deadline is already captured in the `ENQUEUE` record, so replay can filter
out already-expired messages purely from wall-clock time.

**Write policy:** synchronous fsync before returning success to the
caller, and log-before-apply (the WAL write completes before the in-memory
state transition is considered committed). This is the strongest
durability guarantee available without external infrastructure, at a
known, accepted cost to the p95 latency target — the write-cost tradeoff is
called out explicitly rather than hidden. Kept synchronous for v1
deliberately rather than pre-emptively optimized: if load testing shows
the p95 < 100ms NFR isn't met, batched/group-commit fsync (coalesce writes
arriving within a short window into one fsync) is the named follow-up —
see section 16.

**Startup replay:** scan the `wal/` directory, replay each queue's file
independently (no cross-file ordering dependency), reconstructing
`QueueInstance` state from its own event stream.

**Known limitation, explicitly out of scope:** no log compaction. Files
grow unboundedly for the life of the process. Acceptable for this
exercise's scale and runtime; a production version would compact or
rotate + snapshot periodically.

**Durability guarantee, stated plainly for the README's NFR section:** a
message is durable once its `ENQUEUE` (or terminal `ACK`/`DLQ_MOVE`) record
has been fsynced. A crash at any point recovers to a state consistent with
the last fsynced record per queue; in-flight leases at crash time are not
preserved and simply become redeliverable, which is within the at-least-once
contract, not a violation of it.

## 8. Backpressure

Per-tier queues are bounded (default capacity below), enforced via each
tier's `tierCapacityUsed` counter (§4/§5) rather than the tier queue's own
size or the `readyPermits` semaphore — decoupled specifically so a tier
full of TTL-tombstoned-but-not-yet-popped messages doesn't spuriously
reject new enqueues. When a tier is at capacity, `enqueue()` fails
immediately with an explicit rejection rather than blocking — chosen for
v1 simplicity; a blocking-with-timeout variant is a natural extension if
producer-side backpressure handling needs to be gentler than an immediate
error.

## 9. At-least-once delivery, end to end

`Dequeue` leases a message (`READY → IN_FLIGHT`) with the queue's
configured visibility timeout, returning the message's current
`redeliveryCount` alongside its ID as a lease-fencing token (§3.1).
`Acknowledge` takes `(messageId, redeliveryCount)`, transitions `IN_FLIGHT
→ ACKED` and removes the message permanently — but only if the supplied
`redeliveryCount` still matches the record's current value; otherwise the
lease has since been superseded by a redelivery and the ack is a no-op.
If the visibility timeout
elapses without an ack, the per-queue reaper transitions the message back
to `READY` and increments `redeliveryCount`; once `redeliveryCount` exceeds
the queue's configured retry limit, the message instead moves to `DEAD`
(the DLQ) and is never redelivered again. There is no explicit
nack/reject operation — the spec's API surface only exposes
`Acknowledge`, so redelivery is driven purely by visibility-timeout
expiry.

## 10. API surface (adapter)

HTTP via Javalin, JSON bodies. Indicative routing (subject to refinement
during implementation, not a locked contract):

- `POST /queues` — Create Queue (`name`, optional `visibilityTimeoutMs`,
  `retryLimit`, `capacityPerTier`)
- `POST /queues/{name}/messages` — Enqueue (`payload`, `priority`, optional
  `ttlMs`) → returns `messageId`
- `POST /queues/{name}/messages:dequeue?waitMs=N` — Dequeue → returns the
  highest-priority available message, including its current
  `redeliveryCount` (required back on `Acknowledge` as a lease-fencing
  token — see §3.1). Bounded long-poll: blocks up to `waitMs` (default a
  few seconds, server-capped at ~30s) via `tryAcquire(waitMs)` on
  `readyPermits`, returning 200 with an empty result on timeout rather than
  blocking indefinitely or erroring — see §5
- `POST /queues/{name}/messages/{id}:ack` — Acknowledge (body:
  `redeliveryCount`, the value returned by the `Dequeue` that produced this
  lease). Always returns 200 with one of three outcomes, never an error for
  any of them, since a consumer retrying a lost ack response must never see
  a spurious failure for work it already completed: `ACKED` (this call
  performed the transition), `ALREADY_ACKED`/`UNKNOWN` (message already
  terminal or never existed — no-op), or `LEASE_SUPERSEDED` (supplied
  `redeliveryCount` doesn't match current — the lease has since been
  redelivered to someone else — no-op)
- `GET /metrics` — Prometheus-format scrape endpoint, backed by
  Micrometer's `PrometheusMeterRegistry`

## 11. Metrics (Micrometer)

All metrics are maintained incrementally at state-transition time
(`LongAdder`/`AtomicLong`), never derived by traversing a collection —
`Collection.size()` on a linked structure is O(n) and disqualifying for a
polled metric.

- **Oldest Message Age** (per queue, gauge): the min `enqueuedAt` across
  the three tier queues' *ready* heads, skipping any stale head
  (already-expired or already-leased) without mutating other consumers'
  view of the queue — this needs a lazy-expiry-aware peek, not a plain
  `peek()`.
- **Message Count** (per queue, per tier, gauge): maintained counter,
  incremented/decremented alongside enqueue/dequeue/expire.
- **Messages In Flight** (per queue, gauge): maintained counter,
  incremented on dequeue, decremented on ack or expiry-triggered requeue.
- **Throughput** (per queue): `Counter`s on enqueue and ack events; rate is
  computed by the Prometheus scrape (`rate()`), not hand-rolled windowing.
- **Failures / DLQ** (per queue): `Counter`, incremented on every
  `DLQ_MOVE`.

## 12. Horizontal scaling (design narrative — not implemented)

Each queue is owned by exactly one instance at a time; ownership is
resolved via consistent hashing over the queue name (or an external
routing/discovery layer). A request for a queue that lands on the "wrong"
instance is routed to the owning instance. This keeps the in-memory core
engine described above as "the per-shard implementation" rather than
requiring it to become distributed itself — no shared datastore is
required for this model, consistent with staying framework-free and
infra-free.

**Known ceiling of this model:** ownership is per-*whole-queue*, so one
exceptionally hot queue is capped at a single instance's throughput —
there's no path to more capacity for that one queue by adding instances,
only for the fleet's aggregate queue count. The analogous production fix
is intra-queue partitioning (the way Kafka splits one topic into multiple
partitions): split a logical queue into N physical shards, each
independently owned/hashed, at the cost of FIFO-within-tier now only
holding within a shard rather than globally across the whole queue. Not
built here — named as a deliberate non-goal, not an unstated gap.

## 13. Node failure handling (design narrative — not implemented)

Today: a single instance is a single point of failure for the queues it
owns — if it dies, those queues' state (including their WAL files, if not
on shared/replicated storage) is unavailable until it restarts. Production
extension: replicate or ship each queue's WAL file to a standby instance,
which replays it and can take over ownership on failure — this is a
concrete, buildable extension precisely because the WAL already exists as
a well-defined per-queue artifact to ship.

## 14. Testing strategy

- **Deterministic time**: a `Clock` is injected everywhere TTL/visibility
  timeout logic reads the current time — no `Thread.sleep`-based timing
  tests, no direct `System.currentTimeMillis()` calls in core logic.
- **Concurrency invariants**: multithreaded stress tests using
  `CountDownLatch`/`CyclicBarrier` to force concurrent start across many
  producer/consumer threads, asserting invariants such as: no message ID
  is ever handed to two different in-flight leases at once; priority
  ordering holds under concurrent mixed-tier load; no message is lost.
  **Honest limitation:** latch-based stress tests increase confidence but
  passing every run does not prove the absence of a JMM-level reordering
  bug — that needs a tool like `jcstress`, deliberately not used here
  (decision #18) for speed and live-explainability. This is a scope
  tradeoff, not a claim of formal verification, and it's the first thing
  to reach for if the CAS + lease-fencing ordering (§3.1) is ever in doubt.
- **Required edge cases** (named explicitly in the spec's grading
  criteria): DLQ overflow (redelivery count crossing the threshold),
  priority inversion (a LOW message never overtakes a HIGH message that
  was ready first), visibility timeout enforcement, concurrent
  Create-Queue calls.
- **WAL crash-recovery tests**: simulate a crash (kill mid-write / restart
  the in-memory structures from an on-disk log without a clean shutdown),
  replay, and assert reconstructed state matches expectations — this is
  the test evidence backing the durability guarantee in section 7, not
  just prose.

## 15. Config defaults

Queues can be created with `Create Queue` supplying no settings; sensible
defaults apply so the API is usable without requiring every caller to
think through every setting:

- Visibility timeout: 30s
- Retry limit (before DLQ): 5
- Per-tier capacity: 10,000

## 16. Known gaps / what a production version would add

- Lease renewal / heartbeat API (section 6)
- WAL log compaction / rotation (section 7)
- Actual multi-instance failover implementation, not just the narrative in
  section 13 (would build on the WAL's per-queue file as the replication
  unit)
- A public DLQ inspect/replay API (currently internal/test-only)
- Fairness/aging in the dequeue selection policy (the layer exists and is
  swappable, but ships as strict priority in v1)
- Blocking-with-timeout as an alternative to immediate rejection on a full
  queue (section 8)
- Batched/group-commit fsync (section 7) — v1 ships synchronous per-write
  fsync deliberately; only worth the added complexity if load testing shows
  the p95 < 100ms NFR isn't met, so it's held as a measured-not-assumed
  follow-up rather than built preemptively
- Intra-queue partitioning (section 12) — today one queue is capped at one
  instance's throughput; sharding a single logical queue across instances
  (Kafka-style) is the production answer if one queue's hot enough to need it
- `jcstress`-based verification of the CAS + lease-fencing ordering
  guarantees (section 14), as a rigor upgrade over the hand-rolled stress
  harness

## 17. Project layout

- `com.dpqs.core` — the framework-free engine: `QueueManager`,
  `QueueInstance`, `MessageRecord`, the state machine, tier queues, the
  timer/reaper, WAL read/write, config. This is what's evaluated as "the
  core engine."
- `com.dpqs.service` — the Javalin HTTP adapter: routes, request/response
  DTOs, wiring `PrometheusMeterRegistry` to `GET /metrics`.
- `src/test` — unit and concurrency tests (JUnit 5), run by `./gradlew test`.
- `src/integrationTest` — smoke/integration tests (JUnit 5 + REST Assured)
  run over real HTTP against a running, deployed instance, by
  `./gradlew integrationTest`.

## Running locally

```bash
# Unit tests only
./gradlew test

# Run the service (defaults to :8080; override with PORT)
./gradlew run
# in another shell:
curl http://localhost:8080/health

# Integration test against that running instance
SERVICE_BASE_URL=http://localhost:8080 ./gradlew integrationTest
```

## Running via Docker / Compose

```bash
# Build the image
docker build -t distributed-priority-queue-service:local .

# Bring up "staging" (host port 8080) or "prod" (host port 8081) — both
# profiles run the exact same image, parameterized by IMAGE_TAG
IMAGE_TAG=distributed-priority-queue-service:local docker compose --profile staging up -d
curl http://localhost:8080/health

IMAGE_TAG=distributed-priority-queue-service:local docker compose --profile prod up -d
curl http://localhost:8081/health

docker compose --profile staging down
docker compose --profile prod down
```

## CI/CD pipeline (`.github/workflows/ci.yml`)

```
build → deploy-staging → integration-tests-staging → deploy-prod
```

**Build once, promote the same artifact.** `build` compiles, runs unit
tests, builds exactly one Docker image, tags it with the git SHA
(`ghcr.io/<repo>:<sha>`), and pushes it to GHCR. Every later stage
references that same `IMAGE_TAG` — nothing is rebuilt or re-tagged after
this point. "Promotion to prod" means running that identical, already-tested
artifact, not producing a new build from the same source a second time.

**The integration-test gate.** `deploy-prod` declares
`needs: [build, integration-tests-staging]`. If the integration-test job
fails (or is skipped), GitHub Actions skips `deploy-prod` outright — there is
no path from a red pipeline to a prod deployment.

**Honest note on staging/prod.** `deploy-staging`,
`integration-tests-staging`, and `deploy-prod` each spin up the promoted
image as an **ephemeral `docker compose` container on the GitHub Actions
runner itself**, and tear it down at the end of the job. This is not a real,
persistent staging or prod host — there isn't one for this take-home. It's
worth calling out explicitly because of a real GitHub Actions constraint:
each job in a workflow runs on its own isolated, disposable VM, so a
container started in `deploy-staging` cannot be reached from
`integration-tests-staging` (a different job, a different machine).
`integration-tests-staging` therefore deploys its own fresh instance of the
*same* image before testing it — the artifact never changes, only the
disposable container running it does. If/when this pipeline targets real
infrastructure, `deploy-staging`/`deploy-prod` would instead push the image
to an actual long-lived environment (e.g. `kubectl set image`, an ECS
deployment, SSH + compose on a real host), and
`integration-tests-staging` would point `SERVICE_BASE_URL` at that
environment's real address instead of `localhost`.
