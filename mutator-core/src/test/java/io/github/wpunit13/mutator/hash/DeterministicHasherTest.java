package io.github.wpunit13.mutator.hash;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class DeterministicHasherTest {

    // Golden values, pinned independently. Do NOT compute from the implementation.
    private static final String EMPTY_STRING_HASH = "e3b0c44298fc1c14";
    private static final String SPARK_MUTATOR_HASH = "6b4a1c1466633225";
    private static final String ORDERS_JOIN_1_HASH = "5186734e485702b6";

    // Hoisted so the loop below does not recompile the pattern per iteration.
    private static final Pattern LOWERCASE_HEX_16 = Pattern.compile("^[0-9a-f]{16}$");

    @Test
    void hashToHexEmptyStringMatchesGoldenValue() {
        assertEquals(EMPTY_STRING_HASH, DeterministicHasher.hashToHex(""));
    }

    @Test
    void hashToHexSparkMutatorMatchesGoldenValue() {
        assertEquals(SPARK_MUTATOR_HASH, DeterministicHasher.hashToHex("spark-mutator"));
    }

    @Test
    void computeMutantIdMatchesGoldenValue() {
        assertEquals(
                ORDERS_JOIN_1_HASH,
                DeterministicHasher.computeMutantId("src/pipeline/orders.py", "0123456789abcdef", "JOIN", 1));
    }

    @Test
    void outputIsAlways16LowercaseHexChars() {
        List<String> inputs = new ArrayList<>();
        StringBuilder longInput = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            longInput.append((char) ('a' + (i % 26)));
        }
        inputs.add("");
        inputs.add("a");
        inputs.add("some random plan node canonical string");
        inputs.add(longInput.toString());
        inputs.add("multi-byte: caf\u00e9 na\u00efve \u4e2d\u6587 \ud83d\ude00");

        for (String input : inputs) {
            String hash = DeterministicHasher.hashToHex(input);
            assertEquals(16, hash.length(), "hash length for input: " + input);
            assertTrue(LOWERCASE_HEX_16.matcher(hash).matches(), "hash format for input: " + input);
        }
    }

    @Test
    void hashToHexNullThrows() {
        assertThrows(IllegalArgumentException.class, () -> DeterministicHasher.hashToHex(null));
    }
}
