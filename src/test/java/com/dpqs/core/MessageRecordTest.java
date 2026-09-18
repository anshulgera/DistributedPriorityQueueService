package com.dpqs.core;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageRecordTest {

    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

    private static MessageRecord newReadyMessage() {
        return MessageRecord.create("m1", "payload", Priority.HIGH, null, FIXED_CLOCK);
    }

    // --- construction -------------------------------------------------

    @Test
    void startsReadyWithZeroRedeliveries() {
        MessageRecord record = newReadyMessage();
        assertEquals(MessageState.READY, record.state());
        assertEquals(0, record.redeliveryCount());
    }

    @Test
    void enqueuedAtComesFromInjectedClockNotWallClock() {
        MessageRecord record = newReadyMessage();
        assertEquals(FIXED_CLOCK.instant(), record.enqueuedAt());
    }

    @Test
    void ttlDeadlineIsEnqueuedAtPlusTtlWhenProvided() {
        MessageRecord record = MessageRecord.create(
                "m1", "payload", Priority.HIGH, Duration.ofMinutes(5), FIXED_CLOCK);
        assertEquals(Optional.of(FIXED_CLOCK.instant().plus(Duration.ofMinutes(5))),
                record.ttlDeadline());
    }

    @Test
    void ttlDeadlineIsEmptyWhenNoTtlProvided() {
        assertTrue(newReadyMessage().ttlDeadline().isEmpty());
    }

    // --- legal transitions ---------------------------------------------

    @Test
    void tryLeaseMovesReadyToInFlight() {
        MessageRecord record = newReadyMessage();
        Optional<Integer> fencingToken = record.tryLease();
        assertEquals(Optional.of(0), fencingToken);
        assertEquals(MessageState.IN_FLIGHT, record.state());
    }

    @Test
    void tryAckMovesInFlightToAckedWithMatchingFencingToken() {
        MessageRecord record = newReadyMessage();
        int token = record.tryLease().orElseThrow();

        assertEquals(AckResult.ACKED, record.tryAck(token));
        assertEquals(MessageState.ACKED, record.state());
    }

    @Test
    void tryExpireLeaseUnderRetryLimitRedeliversAndIncrementsCount() {
        MessageRecord record = newReadyMessage();
        record.tryLease();

        MessageRecord.ExpiryOutcome outcome = record.tryExpireLease(5).orElseThrow();

        assertEquals(MessageState.READY, outcome.newState());
        assertEquals(1, outcome.newRedeliveryCount());
        assertEquals(MessageState.READY, record.state());
        assertEquals(1, record.redeliveryCount());
    }

    @Test
    void tryExpireLeaseAtRetryLimitMovesToDead() {
        MessageRecord record = newReadyMessage();
        // Exhaust the retry limit of 1: first expiry redelivers, second dead-letters.
        record.tryLease();
        record.tryExpireLease(1);
        record.tryLease();

        MessageRecord.ExpiryOutcome outcome = record.tryExpireLease(1).orElseThrow();

        assertEquals(MessageState.DEAD, outcome.newState());
        assertEquals(MessageState.DEAD, record.state());
    }

    @Test
    void tryExpireTtlMovesReadyToDead() {
        MessageRecord record = newReadyMessage();
        assertTrue(record.tryExpireTtl());
        assertEquals(MessageState.DEAD, record.state());
    }

    // --- illegal transitions are rejected, not errors -------------------

    @Test
    void tryLeaseOnAlreadyInFlightMessageIsRejected() {
        MessageRecord record = newReadyMessage();
        record.tryLease();

        assertTrue(record.tryLease().isEmpty());
        assertEquals(MessageState.IN_FLIGHT, record.state());
    }

    @Test
    void tryLeaseOnTombstonedMessageIsRejected() {
        MessageRecord record = newReadyMessage();
        record.tryExpireTtl();

        assertTrue(record.tryLease().isEmpty());
    }

    @Test
    void tryAckOnNeverLeasedMessageIsSuperseded() {
        MessageRecord record = newReadyMessage();
        assertEquals(AckResult.LEASE_SUPERSEDED, record.tryAck(0));
    }

    @Test
    void tryAckTwiceIsAlreadyAckedOnSecondCall() {
        MessageRecord record = newReadyMessage();
        int token = record.tryLease().orElseThrow();
        record.tryAck(token);

        assertEquals(AckResult.ALREADY_ACKED, record.tryAck(token));
    }

    @Test
    void tryExpireLeaseOnReadyMessageIsRejected() {
        MessageRecord record = newReadyMessage();
        assertTrue(record.tryExpireLease(5).isEmpty());
    }

    @Test
    void tryExpireLeaseOnAckedMessageIsRejected() {
        MessageRecord record = newReadyMessage();
        int token = record.tryLease().orElseThrow();
        record.tryAck(token);

        assertTrue(record.tryExpireLease(5).isEmpty());
    }

    @Test
    void tryExpireTtlOnInFlightMessageIsRejected() {
        MessageRecord record = newReadyMessage();
        record.tryLease();

        assertFalse(record.tryExpireTtl());
        assertEquals(MessageState.IN_FLIGHT, record.state());
    }

    // --- lease fencing: the core correctness guarantee (README §3.1) ---

    @Test
    void staleAckAfterRedeliveryIsSupersededNotAcked() {
        MessageRecord record = newReadyMessage();
        int staleToken = record.tryLease().orElseThrow(); // first consumer's lease, token 0

        record.tryExpireLease(5); // visibility timeout fires; redeliveryCount -> 1
        int freshToken = record.tryLease().orElseThrow(); // second consumer leases it, token 1
        assertEquals(1, freshToken);

        // The first consumer's ack arrives late, still carrying the stale token.
        AckResult staleAckResult = record.tryAck(staleToken);

        assertEquals(AckResult.LEASE_SUPERSEDED, staleAckResult);
        // Crucially: the second consumer's active lease must be untouched.
        assertEquals(MessageState.IN_FLIGHT, record.state());
        assertEquals(1, record.redeliveryCount());
    }

    @Test
    void freshAckAfterRedeliverySucceeds() {
        MessageRecord record = newReadyMessage();
        record.tryLease();
        record.tryExpireLease(5);
        int freshToken = record.tryLease().orElseThrow();

        assertEquals(AckResult.ACKED, record.tryAck(freshToken));
        assertEquals(MessageState.ACKED, record.state());
    }

    // --- concurrency: only one racer may win tryLease --------------------

    @Test
    void concurrentTryLeaseHasExactlyOneWinner() throws InterruptedException {
        MessageRecord record = newReadyMessage();
        int threadCount = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger winners = new AtomicInteger(0);

        try {
            for (int i = 0; i < threadCount; i++) {
                pool.submit(() -> {
                    ready.countDown();
                    await(go);
                    if (record.tryLease().isPresent()) {
                        winners.incrementAndGet();
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

        assertEquals(1, winners.get());
        assertEquals(MessageState.IN_FLIGHT, record.state());
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
