package io.github.wpunit13.mutator;

import io.github.wpunit13.mutator.catalog.MutationCatalogAccess;
import io.github.wpunit13.mutator.catalog.MutationCatalogIo;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MutantBootstrapTest {

    private static final String MUTANT_ID = "0123456789abcdef";

    @TempDir
    Path tempDir;

    @BeforeEach
    void clearState() {
        MutantRegistry.getInstance().reset();
        MutationCatalogAccess.clearForTesting();
    }

    @AfterEach
    void clearRegistryAndProps() {
        MutantRegistry.getInstance().reset();
        MutationCatalogAccess.clearForTesting();
        System.clearProperty(MutantBootstrap.PROP_ACTIVE_MUTANT);
        System.clearProperty(MutantBootstrap.PROP_PHASE);
        System.clearProperty(MutantBootstrap.PROP_OUTPUT_DIRECTORY);
    }

    /** Writes a catalog.json containing MUTANT_ID and points the bridge at it. */
    private void writeCatalog() throws IOException {
        MutantMetadata meta = new MutantMetadata(
                MUTANT_ID,
                "src/main/scala/P.scala",
                1,
                OperatorTypeDto.JOIN,
                2,
                "INNER -> ANTI",
                "9f8e7d6c5b4a3210",
                "",
                List.of());
        MutationCatalogIo.writeCatalogJson(tempDir, List.of(meta));
        System.setProperty(MutantBootstrap.PROP_OUTPUT_DIRECTORY,
                tempDir.toAbsolutePath().toString());
    }

    @Test
    void activatesMutantFromSystemPropertyAndLoadsCatalog() throws IOException {
        writeCatalog();
        System.setProperty(MutantBootstrap.PROP_ACTIVE_MUTANT, MUTANT_ID);

        MutantBootstrap.activateFromSystemProperties();

        assertEquals(MUTANT_ID, MutantRegistry.getInstance().getActiveMutantOrNull());
        // Fork-side catalog handoff: the Catalyst rule resolves the mutant
        // through the in-memory catalog, so it must be present after activation.
        assertNotNull(MutationCatalogAccess.findByIdOrNull(MUTANT_ID));
    }

    @Test
    void activationIsIdempotentForSameMutant() throws IOException {
        writeCatalog();
        System.setProperty(MutantBootstrap.PROP_ACTIVE_MUTANT, MUTANT_ID);
        MutantBootstrap.activateFromSystemProperties();
        MutantBootstrap.activateFromSystemProperties();
        assertEquals(MUTANT_ID, MutantRegistry.getInstance().getActiveMutantOrNull());
    }

    @Test
    void noActiveMutantPropertyLeavesRegistryIdle() {
        MutantBootstrap.activateFromSystemProperties();
        assertNull(MutantRegistry.getInstance().getActiveMutantOrNull());
    }

    @Test
    void activeMutantWithoutCatalogFailsLoudly() {
        // No catalog.json written: a silent no-op would masquerade as "every
        // mutant survived", so activation must fail loudly instead.
        System.setProperty(MutantBootstrap.PROP_ACTIVE_MUTANT, MUTANT_ID);
        System.setProperty(MutantBootstrap.PROP_OUTPUT_DIRECTORY,
                tempDir.toAbsolutePath().toString());
        assertThrows(IllegalStateException.class,
                MutantBootstrap::activateFromSystemProperties);
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
