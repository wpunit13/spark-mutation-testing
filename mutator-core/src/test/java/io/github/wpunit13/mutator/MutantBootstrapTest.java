package io.github.wpunit13.mutator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MutantBootstrapTest {

    @BeforeEach
    void clearRegistry() {
        MutantRegistry.getInstance().reset();
    }

    @AfterEach
    void clearRegistryAndProps() {
        MutantRegistry.getInstance().reset();
        System.clearProperty(MutantBootstrap.PROP_ACTIVE_MUTANT);
        System.clearProperty(MutantBootstrap.PROP_PHASE);
        System.clearProperty(MutantBootstrap.PROP_OUTPUT_DIRECTORY);
    }

    @Test
    void activatesMutantFromSystemProperty() {
        System.setProperty(MutantBootstrap.PROP_ACTIVE_MUTANT, "0123456789abcdef");
        MutantBootstrap.activateFromSystemProperties();
        assertEquals("0123456789abcdef", MutantRegistry.getInstance().getActiveMutantOrNull());
    }

    @Test
    void activationIsIdempotentForSameMutant() {
        System.setProperty(MutantBootstrap.PROP_ACTIVE_MUTANT, "0123456789abcdef");
        MutantBootstrap.activateFromSystemProperties();
        MutantBootstrap.activateFromSystemProperties();
        assertEquals("0123456789abcdef", MutantRegistry.getInstance().getActiveMutantOrNull());
    }

    @Test
    void noActiveMutantPropertyLeavesRegistryIdle() {
        MutantBootstrap.activateFromSystemProperties();
        assertNull(MutantRegistry.getInstance().getActiveMutantOrNull());
    }

    @Test
    void phaseOrNullReturnsNullWhenUnset() {
        assertNull(MutantBootstrap.phaseOrNull());
    }

    @Test
    void phaseOrNullReturnsTrimmedPhase() {
        System.setProperty(MutantBootstrap.PROP_PHASE, "  mutant ");
        assertEquals("mutant", MutantBootstrap.phaseOrNull());
    }
}