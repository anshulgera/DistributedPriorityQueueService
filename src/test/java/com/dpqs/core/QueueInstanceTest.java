package com.dpqs.core;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class QueueInstanceTest {

    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

    private static QueueInstance newInstance(int capacityPerTier) {
        return new QueueInstance(
                new QueueConfig(Duration.ofSeconds(30), 5, capacityPerTier), FIXED_CLOCK);
    }

    // --- happy path -------------------------------------------------------

    @Test
    void enqueueDequeueAckRoundTrip() {
        QueueInstance queue = newInstance(10);
        String id = queue.enqueue("payload", Priority.HIGH, null).orElseThrow();

        QueueInstance.DequeueResult result = queue.dequeue(Duration.ofSeconds(1)).orElseThrow();
        assertEquals(id, result.messageId());
        assertEquals("payload", result.payload());
        assertEquals(Priority.HIGH, result.priority());
        assertEquals(0, result.redeliveryCount());

        assertEquals(AckResult.ACKED, queue.ack(id, result.redeliveryCount()));
    }

    @Test
    void dequeueOnEmptyQueueTimesOutEmpty() {
        QueueInstance queue = newInstance(10);
        assertTrue(queue.dequeue(Duration.ofMillis(50)).isEmpty());
    }

    @Test
    void ackWithWrongFencingTokenIsSuperseded() {
        QueueInstance queue = newInstance(10);
        String id = queue.enqueue("payload", Priority.HIGH, null).orElseThrow();
        queue.dequeue(Duration.ofSeconds(1));

        assertEquals(AckResult.LEASE_SUPERSEDED, queue.ack(id, 99));
    }

    @Test
    void ackOfUnknownMessageIdIsAlreadyAcked() {
        QueueInstance queue = newInstance(10);
        assertEquals(AckResult.ALREADY_ACKED, queue.ack("nonexistent", 0));
    }

    // --- priority ordering --------------------------------------------

    @Test
    void dequeueReturnsHighestPriorityFirstRegardlessOfEnqueueOrder() {
        QueueInstance queue = newInstance(10);
        queue.enqueue("low", Priority.LOW, null);
        queue.enqueue("medium", Priority.MEDIUM, null);
        queue.enqueue("high", Priority.HIGH, null);

        assertEquals("high", queue.dequeue(Duration.ofSeconds(1)).orElseThrow().payload());
        assertEquals("medium", queue.dequeue(Duration.ofSeconds(1)).orElseThrow().payload());
        assertEquals("low", queue.dequeue(Duration.ofSeconds(1)).orElseThrow().payload());
    }

    @Test
    void fifoOrderHoldsWithinATier() {
        QueueInstance queue = newInstance(10);
        queue.enqueue("first", Priority.MEDIUM, null);
        queue.enqueue("second", Priority.MEDIUM, null);
        queue.enqueue("third", Priority.MEDIUM, null);

        assertEquals("first", queue.dequeue(Duration.ofSeconds(1)).orElseThrow().payload());
        assertEquals("second", queue.dequeue(Duration.ofSeconds(1)).orElseThrow().payload());
        assertEquals("third", queue.dequeue(Duration.ofSeconds(1)).orElseThrow().payload());
    }

    // --- backpressure / capacity release on ack ------------------------

    @Test
    void enqueueRejectedWhenTierAtCapacity() {
        QueueInstance queue = newInstance(1);
        assertTrue(queue.enqueue("m1", Priority.HIGH, null).isPresent());
        assertTrue(queue.enqueue("m2", Priority.HIGH, null).isEmpty());
    }

    @Test
    void ackingFreesCapacityForANewEnqueue() {
        QueueInstance queue = newInstance(1);
        String id = queue.enqueue("m1", Priority.HIGH, null).orElseThrow();
        assertTrue(queue.enqueue("m2", Priority.HIGH, null).isEmpty());

        QueueInstance.DequeueResult leased = queue.dequeue(Duration.ofSeconds(1)).orElseThrow();
        queue.ack(id, leased.redeliveryCount());

        assertTrue(queue.enqueue("m2", Priority.HIGH, null).isPresent());
    }

    // --- tombstone-aware discard (README §5), ahead of Phase 3's reaper --

    @Test
    void dequeueSkipsATombstonedMessageAndReturnsTheNextLiveOne() {
        QueueInstance queue = newInstance(10);
        MessageRecord tombstoned =
                MessageRecord.create("dead", "dead-payload", Priority.HIGH, null, FIXED_CLOCK);
        assertTrue(tombstoned.tryExpireTtl()); // simulate the reaper having already fired

        queue.insertForTest(tombstoned);
        String liveId = queue.enqueue("alive", Priority.HIGH, null).orElseThrow();

        QueueInstance.DequeueResult result = queue.dequeue(Duration.ofSeconds(1)).orElseThrow();

        assertEquals(liveId, result.messageId());
        assertEquals("alive", result.payload());
        // The tombstone must not resurface on a later dequeue.
        assertTrue(queue.dequeue(Duration.ofMillis(50)).isEmpty());
    }

    // --- concurrency ------------------------------------------------------

    @Test
    void concurrentDequeuesNeverDeliverTheSameMessageTwice() throws InterruptedException {
        QueueInstance queue = newInstance(1000);
        int messageCount = 300;
        for (int i = 0; i < messageCount; i++) {
            queue.enqueue("m" + i, Priority.values()[i % 3], null);
        }

        int consumerCount = 20;
        ExecutorService pool = Executors.newFixedThreadPool(consumerCount);
        Set<String> delivered = ConcurrentHashMap.newKeySet();
        AtomicInteger deliveredCount = new AtomicInteger(0);

        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < consumerCount; i++) {
                futures.add(pool.submit(() -> {
                    Optional<QueueInstance.DequeueResult> result;
                    while ((result = queue.dequeue(Duration.ofMillis(200))).isPresent()) {
                        boolean firstDelivery = delivered.add(result.get().messageId());
                        assertTrue(firstDelivery, "message delivered twice concurrently");
                        deliveredCount.incrementAndGet();
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            pool.shutdownNow();
        }

        assertEquals(messageCount, deliveredCount.get());
        assertEquals(messageCount, delivered.size());
    }

    /**
     * Deliberately does NOT run concurrent consumers here: with multiple
     * consumer threads racing, the order in which each thread's dequeue()
     * call happens to *return and record its result* is not the same as
     * the true claim order (a thread that legitimately claimed a HIGH
     * message can be scheduled out before it appends to a shared result
     * list, letting a thread that claimed the now-only-remaining LOW
     * message record first) - that's a measurement artifact of concurrent
     * completion timing, not a priority-ordering bug, and asserting on it
     * produces a genuinely flaky test rather than catching a real one.
     * <p>
     * Concurrency is exercised on the producer side instead (racing
     * enqueues across tiers, which is where the real concurrency-sensitive
     * logic lives: capacity CAS, permit release ordering), then a single
     * sequential consumer drains and checks ordering - a property that
     * only has one valid measurement once dequeue is single-threaded.
     */
    @Test
    void priorityOrderingHoldsAfterConcurrentMixedTierEnqueue() throws InterruptedException {
        QueueInstance queue = newInstance(1000);
        int producerCount = 10;
        int perProducerPerTier = 20;
        ExecutorService pool = Executors.newFixedThreadPool(producerCount);
        CountDownLatch ready = new CountDownLatch(producerCount);
        CountDownLatch go = new CountDownLatch(1);

        try {
            for (int p = 0; p < producerCount; p++) {
                int producerId = p;
                pool.submit(() -> {
                    ready.countDown();
                    await(go);
                    for (int i = 0; i < perProducerPerTier; i++) {
                        queue.enqueue("high-" + producerId + "-" + i, Priority.HIGH, null);
                        queue.enqueue("low-" + producerId + "-" + i, Priority.LOW, null);
                    }
                });
            }
            ready.await();
            go.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        boolean seenLow = false;
        int total = 0;
        Optional<QueueInstance.DequeueResult> result;
        while ((result = queue.dequeue(Duration.ofMillis(200))).isPresent()) {
            total++;
            if (result.get().priority() == Priority.LOW) {
                seenLow = true;
            } else if (seenLow) {
                fail("a HIGH message was delivered after a LOW message");
            }
        }
        assertEquals(producerCount * perProducerPerTier * 2, total);
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
