package io.github.wpunit13.mutator.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/**
 * Immutable metadata describing a single mutant produced by the catalog.
 *
 * <p>All fields are constructor-injected and set exactly once. Identity is
 * {@link #equals(Object)} / {@link #hashCode()} over mutantId alone (the
 * canonical identity).
 */
public final class MutantMetadata {

    private final String mutantId;          // 16-char lowercase hex
    private final String filePath;          // project-relative source file of the test/pipeline module
    private final int lineNumber;           // best-effort source line of the mutated call site; -1 if unknown
    private final OperatorTypeDto operatorType; // enum mirror of api.OperatorType (no catalyst import)
    private final int mutationIndex;        // 0-based index within the operator's mutation rule list
    private final String description;       // human-readable, e.g. "INNER -> CROSS"
    private final String coordinateHex;     // NodeCoordinate.toHex(), 16-char lowercase hex
    private final String astDiffSnippet;    // short textual before/after plan fragment, report-facing only
    private final List<String> mappedTestIds; // immutable, from Test Impact Analysis

    public MutantMetadata(
            String mutantId,
            String filePath,
            int lineNumber,
            OperatorTypeDto operatorType,
            int mutationIndex,
            String description,
            String coordinateHex,
            String astDiffSnippet,
            List<String> mappedTestIds) {
        this.mutantId = mutantId;
        this.filePath = filePath;
        this.lineNumber = lineNumber;
        this.operatorType = operatorType;
        this.mutationIndex = mutationIndex;
        this.description = description;
        this.coordinateHex = coordinateHex;
        this.astDiffSnippet = astDiffSnippet;
        // Deduplicate (insertion-ordered) and store unmodifiable; null means empty.
        LinkedHashSet<String> deduped = mappedTestIds == null
                ? new LinkedHashSet<>()
                : new LinkedHashSet<>(mappedTestIds);
        this.mappedTestIds = Collections.unmodifiableList(new ArrayList<>(deduped));
    }

    public String getMutantId() {
        return mutantId;
    }

    public String getFilePath() {
        return filePath;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public OperatorTypeDto getOperatorType() {
        return operatorType;
    }

    public int getMutationIndex() {
        return mutationIndex;
    }

    public String getDescription() {
        return description;
    }

    public String getCoordinateHex() {
        return coordinateHex;
    }

    public String getAstDiffSnippet() {
        return astDiffSnippet;
    }

    public List<String> getMappedTestIds() {
        return mappedTestIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MutantMetadata)) {
            return false;
        }
        return Objects.equals(mutantId, ((MutantMetadata) o).mutantId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(mutantId);
    }
}
