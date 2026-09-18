package com.dpqs.core;

/**
 * Ordered HIGH to LOW; ordinal order is the dequeue selection order
 * (README §5). Deliberately just three tiers, not an arbitrary numeric
 * priority, per the assignment spec.
 */
public enum Priority {
    HIGH,
    MEDIUM,
    LOW
}
