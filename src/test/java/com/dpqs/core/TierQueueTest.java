package com.dpqs.core;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TierQueueTest {

    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

    private static MessageRecord message(String id) {
        return MessageRecord.create(id, "payload", Priority.HIGH, null, FIXED_CLOCK);
    }

    // --- capacity, enforced at the boundary -----------------------------

    @Test
    void tryEnqueueSucceedsUpToCapacityAndRejectsBeyondIt() {
        TierQueue tier = new TierQueue(2);

        assertTrue(tier.tryEnqueue(message("m1")));
        assertTrue(tier.tryEnqueue(message("m2")));
        assertFalse(tier.tryEnqueue(message("m3")));

        assertEquals(2, tier.capacityUsed());
    }

    @Test
    void rejectedEnqueueDoesNotAppearInPollOrder() {
        TierQueue tier = new TierQueue(1);
        tier.tryEnqueue(message("m1"));
        tier.tryEnqueue(message("m2")); // rejected, capacity is 1

        assertEquals("m1", tier.poll().orElseThrow().messageId());
        assertTrue(tier.poll().isEmpty());
    }

    @Test
    void releaseCapacityFreesASlotForANewEnqueue() {
        TierQueue tier = new TierQueue(1);
        tier.tryEnqueue(message("m1"));
        assertFalse(tier.tryEnqueue(message("m2")));

        tier.releaseCapacity();

        assertTrue(tier.tryEnqueue(message("m2")));
        assertEquals(1, tier.capacityUsed());
    }

    @Test
    void pollingDoesNotByItselfFreeCapacity() {
        // A leased (popped) message still occupies its slot until a
        // terminal transition explicitly releases it (README §5) — this
        // is what stops a redelivered message from double-counting.
        TierQueue tier = new TierQueue(1);
        tier.tryEnqueue(message("m1"));

        tier.poll();

        assertEquals(1, tier.capacityUsed());
        assertFalse(tier.tryEnqueue(message("m2")));
    }

    // --- FIFO within a tier ----------------------------------------------

    @Test
    void pollReturnsMessagesInEnqueueOrder() {
        TierQueue tier = new TierQueue(3);
        tier.tryEnqueue(message("m1"));
        tier.tryEnqueue(message("m2"));
        tier.tryEnqueue(message("m3"));

        assertEquals("m1", tier.poll().orElseThrow().messageId());
        assertEquals("m2", tier.poll().orElseThrow().messageId());
        assertEquals("m3", tier.poll().orElseThrow().messageId());
        assertTrue(tier.poll().isEmpty());
    }

    // --- concurrency: capacity holds, no message lost or duplicated ------

    @Test
    void concurrentProducersNeverExceedCapacityAndLoseNoAcceptedMessage() throws InterruptedException {
        int capacity = 50;
        int producers = 200; // more producers than capacity, so rejections are expected
        TierQueue tier = new TierQueue(capacity);
        ExecutorService pool = Executors.newFixedThreadPool(producers);
        CountDownLatch ready = new CountDownLatch(producers);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger(0);

        List<String> ids = IntStream.range(0, producers)
                .mapToObj(i -> "m" + i)
                .collect(Collectors.toList());

        try {
            for (String id : ids) {
                pool.submit(() -> {
                    ready.countDown();
                    await(go);
                    if (tier.tryEnqueue(message(id))) {
                        accepted.incrementAndGet();
                    }
                });
            }
            ready.await();
            go.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        assertEquals(capacity, accepted.get());
        assertEquals(capacity, tier.capacityUsed());

        // Every accepted message is retrievable exactly once, no loss/duplication.
        Set<String> drained = ConcurrentHashMap.newKeySet();
        Optional<MessageRecord> next;
        while ((next = tier.poll()).isPresent()) {
            assertTrue(drained.add(next.get().messageId()), "duplicate delivery from poll()");
        }
        assertEquals(capacity, drained.size());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
