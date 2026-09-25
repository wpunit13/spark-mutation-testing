package io.github.wpunit13.mutator.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * WP-24 governance-gate evaluator: real-failure ERRORED is zero-tolerance by
 * default; the designed not-applied population is ratio-gated; negative knobs
 * disable their checks. Pure counting — no Spark, no catalog.
 *
 * <p>The not-applied ratio can be scoped to an accountable set of operator
 * families ({@code spark.mutator.notAppliedExemptMutators}): exempt families are
 * removed from <em>both</em> the numerator and the denominator, so a noisy
 * family (e.g. {@code PROJECT}) neither trips the gate nor dilutes it.
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

    @Test
    void exemptedFamilyNotAppliedIsNotGated() {
        // 32 not-applied, all PROJECT (exempt) → accountable ratio 0.
        Map<OperatorTypeDto, ReportWriter.FamilyStats> stats = Map.of(
                OperatorTypeDto.PROJECT, new ReportWriter.FamilyStats(100, 32, 0),
                OperatorTypeDto.AGGREGATE, new ReportWriter.FamilyStats(20, 0, 0));
        List<String> violations = ReportWriter.evaluateGateViolations(
                120, 0, 32, 0, 0, 0.20, stats, Set.of(OperatorTypeDto.PROJECT));
        assertTrue(violations.isEmpty(), violations.toString());
    }

    @Test
    void accountableFamilyNotAppliedStillFailsAndNamesTheScope() {
        Map<OperatorTypeDto, ReportWriter.FamilyStats> stats = Map.of(
                OperatorTypeDto.PROJECT, new ReportWriter.FamilyStats(100, 30, 0),
                OperatorTypeDto.FILTER, new ReportWriter.FamilyStats(10, 3, 0));
        List<String> violations = ReportWriter.evaluateGateViolations(
                110, 0, 33, 0, 0, 0.20, stats, Set.of(OperatorTypeDto.PROJECT));
        assertEquals(1, violations.size());
        assertTrue(violations.get(0).contains("3/10"), violations.get(0));
        assertTrue(violations.get(0).contains("excluding [PROJECT]"), violations.get(0));
    }

    @Test
    void exemptedCleanFamilyMustNotDiluteTheAccountableRatio() {
        // Global ratio is 3/110 (passes); scoped to the accountable family it is
        // 3/10 — the exempt family must be dropped from both terms.
        Map<OperatorTypeDto, ReportWriter.FamilyStats> stats = Map.of(
                OperatorTypeDto.PROJECT, new ReportWriter.FamilyStats(100, 0, 0),
                OperatorTypeDto.FILTER, new ReportWriter.FamilyStats(10, 3, 0));
        List<String> violations = ReportWriter.evaluateGateViolations(
                110, 0, 3, 0, 0, 0.20, stats, Set.of(OperatorTypeDto.PROJECT));
        assertEquals(1, violations.size());
        assertTrue(violations.get(0).contains("maxNotAppliedRatio"));
    }

    @Test
    void exemptedFamiliesSkippedMutantsLeaveTheDenominatorToo() {
        // Non-exempt: 6 total − 2 skipped = 4; 3 not-applied / 4 = 0.75 > 0.20.
        Map<OperatorTypeDto, ReportWriter.FamilyStats> stats = Map.of(
                OperatorTypeDto.FILTER, new ReportWriter.FamilyStats(6, 3, 2));
        List<String> violations = ReportWriter.evaluateGateViolations(
                6, 0, 3, 2, 0, 0.20, stats, Set.of(OperatorTypeDto.PROJECT));
        assertEquals(1, violations.size());
        assertTrue(violations.get(0).contains("3/4"), violations.get(0));
    }

    @Test
    void emptyExemptSetReproducesTheGlobalCheck() {
        Map<OperatorTypeDto, ReportWriter.FamilyStats> stats = Map.of(
                OperatorTypeDto.PROJECT, new ReportWriter.FamilyStats(100, 32, 0));
        assertEquals(
                ReportWriter.evaluateGateViolations(100, 0, 32, 0, 0, 0.20),
                ReportWriter.evaluateGateViolations(100, 0, 32, 0, 0, 0.20, stats, Set.of()));
    }

    @Test
    void familyStatsAggregateTotalsPerOperatorFamily() {
        MutantMetadata projA = metadata("aaaaaaaaaaaaaaaa", OperatorTypeDto.PROJECT);
        MutantMetadata projB = metadata("bbbbbbbbbbbbbbbb", OperatorTypeDto.PROJECT);
        MutantMetadata filter = metadata("cccccccccccccccc", OperatorTypeDto.FILTER);
        Map<String, MutantResult> results = Map.of(
                "aaaaaaaaaaaaaaaa", result("aaaaaaaaaaaaaaaa", MutantStatus.NOT_APPLIED),
                "bbbbbbbbbbbbbbbb", result("bbbbbbbbbbbbbbbb", MutantStatus.KILLED));

        // 'filter' has no recorded outcome: it still counts toward the family total.
        Map<OperatorTypeDto, ReportWriter.FamilyStats> stats =
                ReportWriter.familyStats(List.of(projA, projB, filter), results);

        assertEquals(2, stats.get(OperatorTypeDto.PROJECT).getTotal());
        assertEquals(1, stats.get(OperatorTypeDto.PROJECT).getNotApplied());
        assertEquals(0, stats.get(OperatorTypeDto.PROJECT).getSkipped());
        assertEquals(1, stats.get(OperatorTypeDto.FILTER).getTotal());
        assertEquals(0, stats.get(OperatorTypeDto.FILTER).getNotApplied());
    }

    @Test
    void parseOperatorTypesIsCaseInsensitiveAndIgnoresUnknownTokens() {
        assertEquals(Set.of(OperatorTypeDto.PROJECT, OperatorTypeDto.JOIN),
                ReportWriter.parseOperatorTypesCsv(" project , JOIN , not_a_family , "));
    }

    private static MutantMetadata metadata(String id, OperatorTypeDto type) {
        return new MutantMetadata(
                id, "unknown", -1, type, 0, "desc", "0000000000000000", "", List.of());
    }

    private static MutantResult result(String id, MutantStatus status) {
        return new MutantResult(id, status, 1L, null, 0L);
    }
}
