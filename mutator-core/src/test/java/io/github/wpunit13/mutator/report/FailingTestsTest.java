package io.github.wpunit13.mutator.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class FailingTestsTest {

    @Test
    void appendsTheCanonicalBlock() {
        String detail = FailingTests.append(
                "There are test failures.",
                List.of("com.example.FooTest.methodOne", "com.example.FooTest.methodTwo"));
        assertEquals(
                "There are test failures.\n\nFailing tests:\n"
                        + "  - com.example.FooTest.methodOne\n"
                        + "  - com.example.FooTest.methodTwo",
                detail);
    }

    @Test
    void appendWithoutDetailStartsTheBlockDirectly() {
        String detail = FailingTests.append(null, List.of("a.b.C.test"));
        assertEquals("Failing tests:\n  - a.b.C.test", detail);
    }

    @Test
    void appendWithNoIdsIsANoOp() {
        assertEquals("original", FailingTests.append("original", List.of()));
        assertEquals(null, FailingTests.append(null, null));
    }

    @Test
    void parseRoundTripsTheAppendedBlock() {
        String detail = FailingTests.append("boom", List.of("a.b.C.one", "a.b.C.two"));
        assertEquals(List.of("a.b.C.one", "a.b.C.two"), FailingTests.parse(detail));
    }

    @Test
    void parseReturnsEmptyWithoutAttribution() {
        assertTrue(FailingTests.parse(null).isEmpty());
        assertTrue(FailingTests.parse("There are test failures.\nPlease refer to surefire-reports").isEmpty());
        assertTrue(FailingTests.parse("Failing tests:\n").isEmpty());
    }

    @Test
    void parseStopsAtTheFirstNonItemLine() {
        String detail = "Failing tests:\n  - a.b.C.one\nsome trailing line\n  - a.b.C.two";
        assertEquals(List.of("a.b.C.one"), FailingTests.parse(detail));
    }
}
