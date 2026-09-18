package com.dpqs.core;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One priority tier's storage: a lock-free FIFO queue plus an eagerly
 * maintained capacity counter, kept as two separate concerns (README §5,
 * §8).
 *
 * <p>{@code capacityUsed} is deliberately not derived from the queue's own
 * size, and is not freed on {@link #poll()}: a leased (IN_FLIGHT) message
 * still occupies its slot, because a lease-expiry redelivery puts it right
 * back in this tier. A slot is freed only by {@link #releaseCapacity()},
 * called by the owner once a message reaches a genuinely terminal state
 * (ACKED, or DEAD via DLQ threshold or TTL). This is also why a
 * TTL-tombstoned node left sitting in the queue (not yet popped) doesn't
 * spuriously block new enqueues — its slot was already released at the
 * moment it was tombstoned, independent of when some future {@link #poll()}
 * happens to discard the node.
 */
public final class TierQueue {

    private final ConcurrentLinkedQueue<MessageRecord> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger capacityUsed = new AtomicInteger(0);
    private final int capacity;

    public TierQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
    }

    /**
     * Reserves one capacity slot and enqueues the record, or rejects
     * immediately without enqueuing if the tier is already at capacity
     * (README §8 — no blocking on a full tier).
     */
    public boolean tryEnqueue(MessageRecord record) {
        if (!tryReserveCapacity()) {
            return false;
        }
        queue.offer(record);
        return true;
    }

    private boolean tryReserveCapacity() {
        int current;
        do {
            current = capacityUsed.get();
            if (current >= capacity) {
                return false;
            }
        } while (!capacityUsed.compareAndSet(current, current + 1));
        return true;
    }

    /** FIFO removal of this tier's head, or empty if the tier is empty. */
    public Optional<MessageRecord> poll() {
        return Optional.ofNullable(queue.poll());
    }

    /**
     * Frees one capacity slot. Call exactly once per message that belonged
     * to this tier when it reaches a terminal state — never on lease or
     * pop alone, see the class-level note.
     */
    public void releaseCapacity() {
        capacityUsed.decrementAndGet();
    }

    public int capacityUsed() {
        return capacityUsed.get();
    }

    public int capacity() {
        return capacity;
    }
}
