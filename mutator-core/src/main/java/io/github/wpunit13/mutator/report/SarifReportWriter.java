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
     * Any unlisted combination falls back to "UnknownMutator".
     */
    private static final Map<String, String> MUTATOR_NAMES = Map.of(
            "JOIN|0", "JoinTypeToLeftOuterMutator",
            "JOIN|1", "CrossJoinMutator",
            "JOIN|2", "JoinTypeToLeftAntiMutator",
            "FILTER|0", "FilterKeepLeftConjunctMutator",
            "FILTER|1", "FilterKeepRightConjunctMutator",
            "FILTER|2", "FilterAlwaysFalseMutator",
            "FILTER|3", "FilterPredicateInversionMutator");

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
            MutantResult result = results.get(meta.getMutantId());
            if (result == null) {
                continue;
            }
            MutantStatus status = result.getStatus();
            String level;
            if (status == MutantStatus.SURVIVED) {
                level = "warning";
            } else if (status == MutantStatus.ERRORED) {
                level = "error";
            } else {
                // KILLED and TIMED_OUT (and SKIPPED) are omitted from SARIF.
                continue;
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

        Path target = outputDir.resolve(FILE_NAME);
        Files.createDirectories(outputDir);
        Files.writeString(
                target,
                MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n",
                StandardCharsets.UTF_8);
        return target;
    }

    private static String mutatorNameFor(OperatorTypeDto operatorType, int mutationIndex) {
        return MUTATOR_NAMES.getOrDefault(operatorType.name() + "|" + mutationIndex, "UnknownMutator");
    }
}
