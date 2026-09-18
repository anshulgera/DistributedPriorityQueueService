package com.dpqs.core;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * One named queue: enqueue, dequeue (lease), and ack, in memory only.
 * Timers/WAL/metrics are wired in by later phases (README §6, §7, §11) -
 * this class is deliberately usable and fully testable without them.
 */
public final class QueueInstance {

    /** What {@link #dequeue} hands back: the lease and its fencing token. */
    public record DequeueResult(
            String messageId, String payload, Priority priority, int redeliveryCount) {}

    private final QueueConfig config;
    private final Clock clock;
    private final TierSelectionPolicy selectionPolicy;
    private final Map<Priority, TierQueue> tiers;
    // Non-fair: which blocked consumer wakes up first is irrelevant here,
    // since every winner re-polls the shared tiers for the actual
    // highest-priority candidate regardless of acquire order.
    private final Semaphore readyPermits = new Semaphore(0);
    private final ConcurrentHashMap<String, MessageRecord> messages = new ConcurrentHashMap<>();

    public QueueInstance(QueueConfig config, Clock clock) {
        this(config, clock, new StrictPriorityScan());
    }

    public QueueInstance(QueueConfig config, Clock clock, TierSelectionPolicy selectionPolicy) {
        this.config = config;
        this.clock = clock;
        this.selectionPolicy = selectionPolicy;
        this.tiers = new EnumMap<>(Priority.class);
        for (Priority priority : Priority.values()) {
            tiers.put(priority, new TierQueue(config.capacityPerTier()));
        }
    }

    /**
     * @return the new message's id, or empty if its tier is at capacity
     *         (README §8 — rejected immediately, never blocks)
     */
    public Optional<String> enqueue(String payload, Priority priority, Duration ttl) {
        String messageId = UUID.randomUUID().toString();
        MessageRecord record = MessageRecord.create(messageId, payload, priority, ttl, clock);

        if (!tiers.get(priority).tryEnqueue(record)) {
            return Optional.empty();
        }
        messages.put(messageId, record);
        // Must be last: this is what makes the message visible to a
        // concurrent dequeue's tryAcquire.
        readyPermits.release();
        return Optional.of(messageId);
    }

    /**
     * Bounded wait for the highest-priority available message (README §5's
     * long-poll model — the HTTP adapter resolves its own wait budget and
     * passes it straight through here; the semaphore itself never crosses
     * the core/adapter boundary). Empty if nothing became available within
     * {@code maxWait}.
     */
    public Optional<DequeueResult> dequeue(Duration maxWait) {
        Instant deadline = clock.instant().plus(maxWait);
        while (true) {
            Duration remaining = Duration.between(clock.instant(), deadline);
            if (remaining.isNegative() || remaining.isZero()) {
                return Optional.empty();
            }
            if (!tryAcquirePermit(remaining)) {
                return Optional.empty();
            }

            Optional<MessageRecord> candidate = pollNextCandidate();
            if (candidate.isEmpty()) {
                // Permit accounting says a ready message exists somewhere;
                // an empty scan here is a transient visibility race against
                // a concurrent enqueue's queue.offer() rather than a real
                // invariant violation. Loop and try again within budget.
                continue;
            }

            Optional<Integer> fencingToken = candidate.get().tryLease();
            if (fencingToken.isEmpty()) {
                // Tombstoned by a concurrent TTL expiry between poll() and
                // tryLease() (README §5) - this permit is spent on the
                // discard, exactly as intended; loop for another.
                continue;
            }

            MessageRecord record = candidate.get();
            return Optional.of(new DequeueResult(
                    record.messageId(), record.payload(), record.priority(), fencingToken.get()));
        }
    }

    private boolean tryAcquirePermit(Duration wait) {
        try {
            return readyPermits.tryAcquire(wait.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private Optional<MessageRecord> pollNextCandidate() {
        for (Priority priority : selectionPolicy.scanOrder()) {
            Optional<MessageRecord> polled = tiers.get(priority).poll();
            if (polled.isPresent()) {
                return polled;
            }
        }
        return Optional.empty();
    }

    /**
     * @param expectedRedeliveryCount the fencing token from the {@link
     *        DequeueResult} that produced this lease (README §3.1, §9)
     */
    public AckResult ack(String messageId, int expectedRedeliveryCount) {
        MessageRecord record = messages.get(messageId);
        if (record == null) {
            // No fourth AckResult value for "unknown id" - the adapter
            // buckets it with ALREADY_ACKED anyway (README §10), and both
            // mean the same thing to a caller: nothing left to do, not an
            // error.
            return AckResult.ALREADY_ACKED;
        }
        AckResult result = record.tryAck(expectedRedeliveryCount);
        if (result == AckResult.ACKED) {
            tiers.get(record.priority()).releaseCapacity();
        }
        return result;
    }

    public QueueConfig config() {
        return config;
    }

    /**
     * Test-only seam: inserts an already-terminal (e.g. pre-tombstoned)
     * record directly into its tier, bypassing {@link #enqueue}'s public
     * contract. Exists so the dequeue pop-loop's discard path (README §5)
     * is unit-testable without needing a real reaper and real elapsed time
     * (Phase 3) to produce a tombstone.
     */
    void insertForTest(MessageRecord record) {
        tiers.get(record.priority()).tryEnqueue(record);
        readyPermits.release();
    }
}
