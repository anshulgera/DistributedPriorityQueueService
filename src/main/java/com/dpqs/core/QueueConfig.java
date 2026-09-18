package com.dpqs.core;

import java.time.Duration;

/**
 * Per-queue settings supplied to Create Queue (README §10). Defaults match
 * README §15 so a queue is usable without a caller specifying every
 * setting.
 */
public record QueueConfig(Duration visibilityTimeout, int retryLimit, int capacityPerTier) {

    public static final Duration DEFAULT_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);
    public static final int DEFAULT_RETRY_LIMIT = 5;
    public static final int DEFAULT_CAPACITY_PER_TIER = 10_000;

    public QueueConfig {
        if (visibilityTimeout.isNegative() || visibilityTimeout.isZero()) {
            throw new IllegalArgumentException(
                    "visibilityTimeout must be positive: " + visibilityTimeout);
        }
        if (retryLimit < 0) {
            throw new IllegalArgumentException("retryLimit must be non-negative: " + retryLimit);
        }
        if (capacityPerTier <= 0) {
            throw new IllegalArgumentException(
                    "capacityPerTier must be positive: " + capacityPerTier);
        }
    }

    public static QueueConfig defaults() {
        return new QueueConfig(
                DEFAULT_VISIBILITY_TIMEOUT, DEFAULT_RETRY_LIMIT, DEFAULT_CAPACITY_PER_TIER);
    }
}
