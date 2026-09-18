package io.github.wpunit13.mutator.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * WP-24 governance-gate evaluator: real-failure ERRORED is zero-tolerance by
 * default; the designed not-applied population is ratio-gated; negative knobs
 * disable their checks. Pure counting — no Spark, no catalog.
 */
class ReportWriterGateTest {

    private static final int TOTAL = 24;

    @Test
    void zeroToleranceDefaultFailsOnAnyRealFailureErrored() {
        List<String> violations = ReportWriter.evaluateGateViolations(
                TOTAL, 1, 0, 0, 0, 0.20);
        assertEquals(1, violations.size());
        assertTrue(violations.get(0).contains("maxErroredCount"));
    }

    @Test
    void cleanRunPassesBothGates() {
        List<String> violations = ReportWriter.evaluateGateViolations(
                TOTAL, 0, 4, 0, 0, 0.20);
        assertTrue(violations.isEmpty());
    }

    @Test
    void notAppliedRatioOverFloorFails() {
        // 10 / 24 ≈ 0.4167 > 0.20
        List<String> violations = ReportWriter.evaluateGateViolations(
                TOTAL, 0, 10, 0, 0, 0.20);
        assertEquals(1, violations.size());
        assertTrue(violations.get(0).contains("maxNotAppliedRatio"));
    }

    @Test
    void notAppliedRatioAtFloorPasses() {
        // 4 / 20 = 0.20 — exactly at the floor is not a violation.
        List<String> violations = ReportWriter.evaluateGateViolations(
                20, 0, 4, 0, 0, 0.20);
        assertTrue(violations.isEmpty());
    }

    @Test
    void skippedMutantsAreExcludedFromTheRatioDenominator() {
        // 5 not-applied of (24 - 4 skipped) = 0.25 > 0.20 — the skipped
        // mutants must not dilute the denominator.
        List<String> violations = ReportWriter.evaluateGateViolations(
                24, 0, 5, 4, 0, 0.20);
        assertEquals(1, violations.size());
        assertTrue(violations.get(0).contains("maxNotAppliedRatio"));
    }

    @Test
    void negativeKnobsDisableTheirChecks() {
        List<String> violations = ReportWriter.evaluateGateViolations(
                TOTAL, 5, 12, 0, -1, -1.0);
        assertTrue(violations.isEmpty());
    }

    @Test
    void bothGatesCanFailTogether() {
        List<String> violations = ReportWriter.evaluateGateViolations(
                TOTAL, 3, 10, 0, 0, 0.20);
        assertEquals(2, violations.size());
    }
}
