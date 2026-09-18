package com.dpqs.core;

/**
 * README §3.1's state machine:
 *
 * <pre>
 * READY --dequeue--> IN_FLIGHT --ack--> ACKED (terminal)
 *    |                   |
 *    |                   +--lease expiry, count < limit--> READY (redeliveryCount++)
 *    |                   +--lease expiry, count >= limit--> DEAD (terminal, DLQ)
 *    +--TTL expiry--------------------------------------> DEAD (terminal, tombstoned)
 * </pre>
 */
public enum MessageState {
    READY,
    IN_FLIGHT,
    ACKED,
    DEAD
}
