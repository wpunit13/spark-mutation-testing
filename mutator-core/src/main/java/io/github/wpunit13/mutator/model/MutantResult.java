package io.github.wpunit13.mutator.model;

import java.util.Objects;

/**
 * Immutable outcome record for an evaluated mutant, constructed exactly
 * once by ReportSink.recordOutcome.
 *
 * <p>Identity is {@link #equals(Object)} / {@link #hashCode()} over
 * mutantId alone.
 */
public final class MutantResult {

    private final String mutantId;
    private final MutantStatus status;
    private final long elapsedMillis;
    private final String failureDetailOrNull;
    private final long recordedAtEpochMillis;

    public MutantResult(
            String mutantId,
            MutantStatus status,
            long elapsedMillis,
            String failureDetailOrNull,
            long recordedAtEpochMillis) {
        this.mutantId = mutantId;
        this.status = status;
        this.elapsedMillis = elapsedMillis;
        this.failureDetailOrNull = failureDetailOrNull;
        this.recordedAtEpochMillis = recordedAtEpochMillis;
    }

    public String getMutantId() {
        return mutantId;
    }

    public MutantStatus getStatus() {
        return status;
    }

    public long getElapsedMillis() {
        return elapsedMillis;
    }

    public String getFailureDetailOrNull() {
        return failureDetailOrNull;
    }

    public long getRecordedAtEpochMillis() {
        return recordedAtEpochMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MutantResult)) {
            return false;
        }
        return Objects.equals(mutantId, ((MutantResult) o).mutantId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(mutantId);
    }
}
