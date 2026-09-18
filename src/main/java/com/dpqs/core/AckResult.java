package com.dpqs.core;

/**
 * Outcome of {@link MessageRecord#tryAck(int)}. Every outcome is a
 * successful, non-error response to the caller (README §10) — a consumer
 * retrying a lost ack response must never see a spurious failure for work
 * it already completed.
 */
public enum AckResult {
    /** This call performed the READY-lease transition IN_FLIGHT -> ACKED. */
    ACKED,
    /** The message was already ACKED by a previous call; no-op. */
    ALREADY_ACKED,
    /**
     * The supplied redeliveryCount no longer matches the message's current
     * lease generation — it has since expired and been redelivered (or
     * dead-lettered) to a different lease holder. No-op: this ack does not
     * touch the message a different consumer now owns (README §3.1).
     */
    LEASE_SUPERSEDED
}
