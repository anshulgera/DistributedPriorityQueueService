package com.dpqs.core;

import java.util.List;

/** Always HIGH, then MEDIUM, then LOW — no fairness/aging (README §5). */
public final class StrictPriorityScan implements TierSelectionPolicy {

    private static final List<Priority> ORDER =
            List.of(Priority.HIGH, Priority.MEDIUM, Priority.LOW);

    @Override
    public List<Priority> scanOrder() {
        return ORDER;
    }
}
