package com.dpqs.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueueConfigTest {

    @Test
    void defaultsMatchReadmeSpecifiedValues() {
        QueueConfig config = QueueConfig.defaults();

        assertEquals(Duration.ofSeconds(30), config.visibilityTimeout());
        assertEquals(5, config.retryLimit());
        assertEquals(10_000, config.capacityPerTier());
    }

    @Test
    void rejectsNonPositiveVisibilityTimeout() {
        assertThrows(IllegalArgumentException.class,
                () -> new QueueConfig(Duration.ZERO, 5, 10_000));
        assertThrows(IllegalArgumentException.class,
                () -> new QueueConfig(Duration.ofSeconds(-1), 5, 10_000));
    }

    @Test
    void rejectsNegativeRetryLimit() {
        assertThrows(IllegalArgumentException.class,
                () -> new QueueConfig(Duration.ofSeconds(30), -1, 10_000));
    }

    @Test
    void rejectsNonPositiveCapacityPerTier() {
        assertThrows(IllegalArgumentException.class,
                () -> new QueueConfig(Duration.ofSeconds(30), 5, 0));
    }
}
