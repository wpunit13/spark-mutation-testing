package io.github.wpunit13.mutator.report;

import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReportWriterComputeScoreTest {

    private static MutantMetadata meta(String mutantId) {
        return new MutantMetadata(
                mutantId,
                "src/main/scala/P.scala",
                1,
                OperatorTypeDto.JOIN,
                0,
                "INNER -> CROSS",
                "0123456789abcdef",
                "",
                List.of());
    }

    private static MutantResult result(String mutantId, MutantStatus status) {
        return new MutantResult(mutantId, status, 1L, null, 1L);
    }

    @Test
    void killedAndSurvivedYieldFiftyPercent() {
        double score = ReportWriter.computeScore(
                List.of(meta("1111111111111111"), meta("2222222222222222")),
                Map.of(
                        "1111111111111111", result("1111111111111111", MutantStatus.KILLED),
                        "2222222222222222", result("2222222222222222", MutantStatus.SURVIVED)));
        assertEquals(50.0, score, 0.001);
    }

    @Test
    void timedOutCountsAsKilledForScoring() {
        double score = ReportWriter.computeScore(
                List.of(meta("1111111111111111"), meta("2222222222222222"), meta("3333333333333333")),
                Map.of(
                        "1111111111111111", result("1111111111111111", MutantStatus.KILLED),
                        "2222222222222222", result("2222222222222222", MutantStatus.TIMED_OUT),
                        "3333333333333333", result("3333333333333333", MutantStatus.SURVIVED)));
        // (1 killed + 1 timedOut) / (1 + 1 + 1) = 66.67%
        assertEquals(66.67, score, 0.001);
    }

    @Test
    void erroredAndMissingOutcomeAreExcluded() {
        double score = ReportWriter.computeScore(
                List.of(meta("1111111111111111"), meta("2222222222222222"), meta("3333333333333333")),
                Map.of(
                        "1111111111111111", result("1111111111111111", MutantStatus.KILLED),
                        "2222222222222222", result("2222222222222222", MutantStatus.ERRORED)));
        // Only KILLED is scorable here; missing '333...' is excluded. 1/1 = 100%.
        assertEquals(100.0, score, 0.001);
    }

    @Test
    void emptyCatalogYieldsZero() {
        assertEquals(0.0, ReportWriter.computeScore(List.of(), Map.of()), 0.001);
    }
}