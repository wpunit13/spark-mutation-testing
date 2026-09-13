package io.github.wpunit13.mutator.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.wpunit13.mutator.model.MutantMetadata;
import io.github.wpunit13.mutator.model.MutantResult;
import io.github.wpunit13.mutator.model.MutantStatus;
import io.github.wpunit13.mutator.model.OperatorTypeDto;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Writes the SARIF 2.1.0 artifact, derived entirely from the same mutants[]
 * data as mutation-report.json (no independent data collection).
 *
 * <p>SARIF is a CI-gating surfaced-problems format: only SURVIVED
 * (level "warning") and ERRORED (level "error") mutants are emitted;
 * KILLED and TIMED_OUT mutants are omitted entirely.
 */
public final class SarifReportWriter {

    static final String FILE_NAME = "mutation-report.sarif";

    /**
     * Fixed lookup table: operatorType + mutationIndex -> mutator name.
     * Covers every (operatorType, mutationIndex) the Spark 3.5 shim's
     * classify() can emit; any unlisted combination falls back to
     * "UnknownMutator". Kept in sync with the pytest plugin's _MUTATOR_NAMES
     * duplicate and scripts/validate_sarif.py.
     */
    private static final Map<String, String> MUTATOR_NAMES = Map.ofEntries(
            Map.entry("JOIN|0", "JoinTypeToLeftOuterMutator"),
            Map.entry("JOIN|1", "CrossJoinMutator"),
            Map.entry("JOIN|2", "JoinTypeToLeftAntiMutator"),
            Map.entry("FILTER|0", "FilterKeepLeftConjunctMutator"),
            Map.entry("FILTER|1", "FilterKeepRightConjunctMutator"),
            Map.entry("FILTER|2", "FilterAlwaysFalseMutator"),
            Map.entry("FILTER|3", "FilterPredicateInversionMutator"),
            Map.entry("AGGREGATE|0", "AggregateSwapFunctionMutator"),
            Map.entry("AGGREGATE|1", "AggregateDropGroupingKeyMutator"),
            Map.entry("AGGREGATE|2", "AggregateZeroMutator"),
            Map.entry("WINDOW|0", "WindowOrderInversionMutator"),
            Map.entry("WINDOW|1", "WindowFrameTruncationMutator"),
            Map.entry("PROJECT|0", "ProjectCoalesceBypassMutator"),
            Map.entry("PROJECT|1", "ProjectInjectNullMutator"));

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SarifReportWriter() {
    }

    static Path write(
            Path outputDir,
            Collection<MutantMetadata> catalog,
            Map<String, MutantResult> results) throws IOException {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("$schema", "https://json.schemastore.org/sarif-2.1.0.json");
        root.put("version", "2.1.0");

        ArrayNode runs = root.putArray("runs");
        ObjectNode run = runs.addObject();
        ObjectNode tool = run.putObject("tool");
        ObjectNode driver = tool.putObject("driver");
        driver.put("name", "spark-mutator");

        ArrayNode sarifResults = run.putArray("results");
        List<MutantMetadata> sorted = catalog.stream()
                .sorted(Comparator.comparing(MutantMetadata::getMutantId))
                .toList();
        for (MutantMetadata meta : sorted) {
            appendResultIfReportable(sarifResults, results.get(meta.getMutantId()), meta);
        }

        Path target = outputDir.resolve(FILE_NAME);
        Files.createDirectories(outputDir);
        Files.writeString(
                target,
                MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n",
                StandardCharsets.UTF_8);
        return target;
    }

    /**
     * Emits one SARIF result for SURVIVED ("warning") and ERRORED ("error")
     * mutants; every other status (and any catalogued mutant with no recorded
     * outcome) is omitted.
     */
    private static void appendResultIfReportable(
            ArrayNode sarifResults,
            MutantResult result,
            MutantMetadata meta) {
        if (result == null) {
            return;
        }
        MutantStatus status = result.getStatus();
        String level;
        if (status == MutantStatus.SURVIVED) {
            level = "warning";
        } else if (status == MutantStatus.ERRORED) {
            level = "error";
        } else {
            // KILLED and TIMED_OUT (and SKIPPED) are omitted from SARIF.
            return;
        }
        ObjectNode sarifResult = sarifResults.addObject();
        sarifResult.put("ruleId", mutatorNameFor(meta.getOperatorType(), meta.getMutationIndex()));
        sarifResult.put("level", level);
        ObjectNode message = sarifResult.putObject("message");
        message.put("text", meta.getDescription());
        ArrayNode locations = sarifResult.putArray("locations");
        ObjectNode location = locations.addObject();
        ObjectNode physicalLocation = location.putObject("physicalLocation");
        ObjectNode artifactLocation = physicalLocation.putObject("artifactLocation");
        artifactLocation.put("uri", meta.getFilePath());
        // SARIF requires startLine >= 1; omit region entirely for unknown lines.
        if (meta.getLineNumber() >= 1) {
            ObjectNode region = physicalLocation.putObject("region");
            region.put("startLine", meta.getLineNumber());
        }
    }

    private static String mutatorNameFor(OperatorTypeDto operatorType, int mutationIndex) {
        return MUTATOR_NAMES.getOrDefault(operatorType.name() + "|" + mutationIndex, "UnknownMutator");
    }
}
