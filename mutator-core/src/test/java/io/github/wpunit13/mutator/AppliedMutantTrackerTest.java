package io.github.wpunit13.mutator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AppliedMutantTrackerTest {

    @AfterEach
    void resetState() {
        AppliedMutantTracker.clear();
    }

    @Test
    void lastOrNullIsNullUntilARewriteIsApplied() {
        assertNull(AppliedMutantTracker.lastOrNull());
    }

    @Test
    void recordMakesTheIdVisibleViaLastOrNull() {
        AppliedMutantTracker.record("0123456789abcdef");
        assertEquals("0123456789abcdef", AppliedMutantTracker.lastOrNull());
    }

    @Test
    void recordOverwritesThePreviouslyRecordedId() {
        AppliedMutantTracker.record("0123456789abcdef");
        AppliedMutantTracker.record("ffffffffffffffff");
        assertEquals("ffffffffffffffff", AppliedMutantTracker.lastOrNull());
    }

    @Test
    void clearForgetsTheRecordedId() {
        AppliedMutantTracker.record("0123456789abcdef");
        AppliedMutantTracker.clear();
        assertNull(AppliedMutantTracker.lastOrNull());
    }

    @Test
    void recordRejectsNullBlankAndMalformedIds() {
        assertThrows(IllegalArgumentException.class, () -> AppliedMutantTracker.record(null));
        assertThrows(IllegalArgumentException.class, () -> AppliedMutantTracker.record(""));
        assertThrows(IllegalArgumentException.class, () -> AppliedMutantTracker.record("   "));
        assertThrows(IllegalArgumentException.class, () -> AppliedMutantTracker.record("0123456789abcde"));
        assertThrows(IllegalArgumentException.class, () -> AppliedMutantTracker.record("0123456789abcdef0"));
        assertThrows(IllegalArgumentException.class, () -> AppliedMutantTracker.record("0123456789ABCDEF"));
        assertThrows(IllegalArgumentException.class, () -> AppliedMutantTracker.record("zzzzzzzzzzzzzzzz"));
        assertNull(AppliedMutantTracker.lastOrNull(), "a rejected id must not be recorded");
    }
}