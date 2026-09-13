package io.github.wpunit13.mutator.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class MutantMetadataTest {

    private MutantMetadata metadata(List<String> mappedTestIds, String description) {
        return new MutantMetadata(
                "0123456789abcdef",
                "src/pipeline/orders.py",
                42,
                OperatorTypeDto.JOIN,
                1,
                description,
                "fedcba9876543210",
                "Join INNER -> CROSS",
                mappedTestIds);
    }

    @Test
    void mappedTestIdsAreDeduplicatedAndInsertionOrdered() {
        MutantMetadata meta = metadata(List.of("t1", "t2", "t1"), "INNER -> CROSS");
        assertEquals(List.of("t1", "t2"), meta.getMappedTestIds());
    }

    @Test
    void mappedTestIdsAreUnmodifiable() {
        MutantMetadata meta = metadata(List.of("t1"), "INNER -> CROSS");
        List<String> mappedTestIds = meta.getMappedTestIds();
        assertThrows(UnsupportedOperationException.class, () -> mappedTestIds.add("t3"));
    }

    @Test
    void nullMappedTestIdsYieldsEmptyList() {
        MutantMetadata meta = metadata(null, "INNER -> CROSS");
        assertTrue(meta.getMappedTestIds().isEmpty());
    }

    @Test
    void equalsAndHashCodeAreDefinedOverMutantIdAlone() {
        MutantMetadata first = metadata(List.of("t1"), "INNER -> CROSS");
        MutantMetadata second = metadata(List.of("t2"), "INNER -> LEFT");
        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }
}
