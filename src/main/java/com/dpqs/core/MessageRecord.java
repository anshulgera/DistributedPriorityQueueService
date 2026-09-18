package com.dpqs.core;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A single message's identity, payload, and lifecycle state (README §3.1,
 * §4). {@code state} and {@code redeliveryCount} change together, atomically,
 * as one {@link Lease} value — they can't be two independently-CASed fields,
 * because {@code redeliveryCount} doubles as a fencing token: {@link #tryAck}
 * has to check both "is a lease active" and "is it *this* lease" in one
 * indivisible step, or a late ack from a superseded lease could race a
 * genuine ack/redelivery and corrupt state (README §3.1's lease-fencing
 * note).
 *
 * <p>All transition methods are single CAS attempts, never retry loops: in
 * this state machine, a lost CAS always means a different thread already
 * resolved the message (acked it, redelivered it, or dead-lettered it), so
 * the loser's correct behavior is to report that outcome, not to recompute
 * and retry (README §3.1, §6's tombstone note).
 */
public final class MessageRecord {

    /** Immutable (state, redeliveryCount) pair, swapped atomically as one unit. */
    private record Lease(MessageState state, int redeliveryCount) {}

    /** Outcome of a lease-expiry transition (README §6's reaper actions). */
    public record ExpiryOutcome(MessageState newState, int newRedeliveryCount) {}

    private final String messageId;
    private final String payload;
    private final Priority priority;
    private final Instant enqueuedAt;
    private final Instant ttlDeadline; // nullable: no TTL configured
    private final AtomicReference<Lease> lease;

    private MessageRecord(String messageId, String payload, Priority priority,
                           Instant enqueuedAt, Instant ttlDeadline) {
        this.messageId = messageId;
        this.payload = payload;
        this.priority = priority;
        this.enqueuedAt = enqueuedAt;
        this.ttlDeadline = ttlDeadline;
        this.lease = new AtomicReference<>(new Lease(MessageState.READY, 0));
    }

    /**
     * @param ttl nullable — no expiration if absent, per the spec's
     *            "optional TTL" (README §1)
     * @param clock injected, never {@link System#currentTimeMillis()}
     *              directly, so enqueue-time tests are deterministic
     *              (README §14)
     */
    public static MessageRecord create(String messageId, String payload, Priority priority,
                                        Duration ttl, Clock clock) {
        Instant now = clock.instant();
        Instant deadline = ttl == null ? null : now.plus(ttl);
        return new MessageRecord(messageId, payload, priority, now, deadline);
    }

    public String messageId() {
        return messageId;
    }

    public String payload() {
        return payload;
    }

    public Priority priority() {
        return priority;
    }

    public Instant enqueuedAt() {
        return enqueuedAt;
    }

    public Optional<Instant> ttlDeadline() {
        return Optional.ofNullable(ttlDeadline);
    }

    public MessageState state() {
        return lease.get().state();
    }

    public int redeliveryCount() {
        return lease.get().redeliveryCount();
    }

    /**
     * READY -> IN_FLIGHT. Returns the redeliveryCount to hand back to the
     * caller as this lease's fencing token (README §3.1, §9), or empty if
     * the message wasn't READY (already leased, or tombstoned DEAD by a
     * concurrent TTL expiry — README §5's tombstone-aware pop-loop note;
     * the caller is expected to discard and try the next candidate).
     */
    public Optional<Integer> tryLease() {
        Lease current = lease.get();
        if (current.state() != MessageState.READY) {
            return Optional.empty();
        }
        Lease next = new Lease(MessageState.IN_FLIGHT, current.redeliveryCount());
        return lease.compareAndSet(current, next)
                ? Optional.of(next.redeliveryCount())
                : Optional.empty();
    }

    /**
     * IN_FLIGHT -> ACKED, but only if {@code expectedRedeliveryCount}
     * matches the record's current lease generation (README §3.1's
     * fencing rule) — otherwise this ack belongs to a lease that's since
     * been superseded and must not touch the record a different consumer
     * now holds.
     */
    public AckResult tryAck(int expectedRedeliveryCount) {
        Lease current = lease.get();
        if (current.state() == MessageState.ACKED) {
            return AckResult.ALREADY_ACKED;
        }
        if (current.state() != MessageState.IN_FLIGHT
                || current.redeliveryCount() != expectedRedeliveryCount) {
            return AckResult.LEASE_SUPERSEDED;
        }
        Lease next = new Lease(MessageState.ACKED, current.redeliveryCount());
        if (lease.compareAndSet(current, next)) {
            return AckResult.ACKED;
        }
        // Lost the race to a concurrent transition (e.g. the reaper expired
        // this exact lease at the same instant) - report precisely rather
        // than assuming which one it was.
        return lease.get().state() == MessageState.ACKED
                ? AckResult.ALREADY_ACKED
                : AckResult.LEASE_SUPERSEDED;
    }

    /**
     * IN_FLIGHT -> READY (redeliveryCount++) if under {@code retryLimit},
     * else IN_FLIGHT -> DEAD. Called by the reaper on visibility-timeout
     * expiry (README §6). Empty if the message is no longer IN_FLIGHT — it
     * was already acked, so this expiry is a stale, no-op tombstone firing
     * (decision #6).
     */
    public Optional<ExpiryOutcome> tryExpireLease(int retryLimit) {
        Lease current = lease.get();
        if (current.state() != MessageState.IN_FLIGHT) {
            return Optional.empty();
        }
        int nextCount = current.redeliveryCount() + 1;
        MessageState nextState = nextCount > retryLimit ? MessageState.DEAD : MessageState.READY;
        Lease next = new Lease(nextState, nextCount);
        if (!lease.compareAndSet(current, next)) {
            return Optional.empty();
        }
        return Optional.of(new ExpiryOutcome(nextState, nextCount));
    }

    /**
     * READY -> DEAD: a message whose TTL fired while it was still sitting
     * unleased. Leaves the record as a tombstone in place rather than
     * requiring removal from its tier queue (README §5's TTL/permit
     * reconciliation note). False if the message is no longer READY (it
     * was leased, or already resolved, in the meantime) — no-op.
     */
    public boolean tryExpireTtl() {
        Lease current = lease.get();
        if (current.state() != MessageState.READY) {
            return false;
        }
        Lease next = new Lease(MessageState.DEAD, current.redeliveryCount());
        return lease.compareAndSet(current, next);
    }
}
