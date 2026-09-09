package io.github.wpunit13.mutator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MutantRegistryTest {

    private static final String MUTANT_A = "0123456789abcdef";
    private static final String MUTANT_B = "fedcba9876543210";

    private MutantRegistry registry;

    @BeforeEach
    void resetRegistry() {
        registry = MutantRegistry.getInstance();
        registry.reset();
    }

    @Test
    void getInstanceReturnsSameInstance() {
        assertSame(MutantRegistry.getInstance(), MutantRegistry.getInstance());
    }

    @Test
    void freshRegistryIsIdle() {
        assertNull(registry.getActiveMutantOrNull());
    }

    @Test
    void setActiveMutantThenReadBack() {
        registry.setActiveMutant(MUTANT_A);
        assertEquals(MUTANT_A, registry.getActiveMutantOrNull());
    }

    @Test
    void setActiveMutantWhileAlreadyActiveThrows() {
        registry.setActiveMutant(MUTANT_A);
        assertThrows(IllegalStateException.class, () -> registry.setActiveMutant(MUTANT_B));
        // original state preserved
        assertEquals(MUTANT_A, registry.getActiveMutantOrNull());
    }

    @Test
    void clearActiveMutantWithMismatchedIdThrows() {
        registry.setActiveMutant(MUTANT_A);
        assertThrows(IllegalStateException.class, () -> registry.clearActiveMutant(MUTANT_B));
    }

    @Test
    void clearActiveMutantWhileIdleThrows() {
        assertThrows(IllegalStateException.class, () -> registry.clearActiveMutant(MUTANT_A));
    }

    @Test
    void clearActiveMutantNullWhileIdleThrows() {
        // Validation contract: null/malformed ids are rejected with
        // IllegalArgumentException before any state transition is attempted,
        // mirroring setActiveMutant.
        assertThrows(IllegalArgumentException.class, () -> registry.clearActiveMutant(null));
    }

    @Test
    void clearActiveMutantNullWhileActiveThrows() {
        registry.setActiveMutant(MUTANT_A);
        assertThrows(IllegalArgumentException.class, () -> registry.clearActiveMutant(null));
        assertEquals(MUTANT_A, registry.getActiveMutantOrNull());
    }

    @Test
    void clearActiveMutantMalformedWhileActiveThrowsAndStateIsPreserved() {
        registry.setActiveMutant(MUTANT_A);
        assertThrows(IllegalArgumentException.class, () -> registry.clearActiveMutant("NOTHEX"));
        assertEquals(MUTANT_A, registry.getActiveMutantOrNull());
    }

    @Test
    void clearActiveMutantWithCorrectIdReturnsToIdle() {
        registry.setActiveMutant(MUTANT_A);
        registry.clearActiveMutant(MUTANT_A);
        assertNull(registry.getActiveMutantOrNull());
    }

    @Test
    void resetFromActiveYieldsIdle() {
        registry.setActiveMutant(MUTANT_A);
        registry.reset();
        assertNull(registry.getActiveMutantOrNull());
    }

    @Test
    void resetFromIdleIsIdempotent() {
        registry.reset();
        assertNull(registry.getActiveMutantOrNull());
        registry.reset();
        assertNull(registry.getActiveMutantOrNull());
    }

    @Test
    void setActiveMutantRejectsInvalidIds() {
        assertThrows(IllegalArgumentException.class, () -> registry.setActiveMutant(null));
        assertThrows(IllegalArgumentException.class, () -> registry.setActiveMutant(""));
        assertThrows(IllegalArgumentException.class, () -> registry.setActiveMutant("   "));
        assertThrows(IllegalArgumentException.class, () -> registry.setActiveMutant("NOTHEX"));
        assertThrows(IllegalArgumentException.class, () -> registry.setActiveMutant("0123456789ABCDEF"));
        assertThrows(IllegalArgumentException.class, () -> registry.setActiveMutant("0123456789abcde"));
        assertThrows(IllegalArgumentException.class, () -> registry.setActiveMutant("0123456789abcdef0"));
        // invalid attempts must not have changed state
        assertNull(registry.getActiveMutantOrNull());
    }
}
