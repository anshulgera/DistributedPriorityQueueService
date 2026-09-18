package com.dpqs.core;

import java.util.List;

/**
 * Decides which tier {@link QueueInstance#dequeue} tries next. Isolated as
 * its own component, not inlined into the dequeue loop, so fairness/aging
 * (README §5 — e.g. promoting a LOW message after a wait threshold) can
 * replace strict priority later without touching {@link QueueInstance} or
 * {@link TierQueue}. v1 ships only {@link StrictPriorityScan}.
 */
public interface TierSelectionPolicy {
    /** Tiers in the order a dequeue attempt should poll them. */
    List<Priority> scanOrder();
}
