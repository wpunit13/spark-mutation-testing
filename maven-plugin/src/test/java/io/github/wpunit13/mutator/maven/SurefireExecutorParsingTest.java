package io.github.wpunit13.mutator.maven;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SurefireExecutorParsingTest {

    @TempDir
    Path tempDir;

    @Test
    void parsesExecutedAndFailedTestIdsFromSurefireXml() throws Exception {
        Files.writeString(tempDir.resolve("TEST-pipeline.FooTest.xml"), """
                <testsuite name="pipeline.FooTest" tests="3" failures="1" errors="1" skipped="0">
                  <testcase name="passes" classname="pipeline.FooTest" time="0.01"/>
                  <testcase name="fails" classname="pipeline.FooTest" time="0.01">
                    <failure message="expected: 42" type="java.lang.AssertionError">boom</failure>
                  </testcase>
                  <testcase name="errors" classname="pipeline.FooTest" time="0.01">
                    <error message="kaboom" type="java.lang.IllegalStateException">kaboom</error>
                  </testcase>
                </testsuite>
                """);

        SurefireExecutor.SurefireTestResults results = SurefireExecutor.parseSurefireReports(tempDir);

        assertEquals(
                List.of("pipeline.FooTest.errors", "pipeline.FooTest.fails", "pipeline.FooTest.passes"),
                results.executedTestIds());
        assertEquals(
                List.of("pipeline.FooTest.errors", "pipeline.FooTest.fails"),
                results.failedTestIds());
    }

    @Test
    void malformedXmlIsSkippedNotFatal() throws Exception {
        Files.writeString(tempDir.resolve("TEST-pipeline.BadTest.xml"), "<testsuite><testcase ");
        Files.writeString(tempDir.resolve("TEST-pipeline.GoodTest.xml"), """
                <testsuite name="pipeline.GoodTest" tests="1">
                  <testcase name="ok" classname="pipeline.GoodTest" time="0.01"/>
                </testsuite>
                """);

        SurefireExecutor.SurefireTestResults results = SurefireExecutor.parseSurefireReports(tempDir);

        assertEquals(List.of("pipeline.GoodTest.ok"), results.executedTestIds());
        assertTrue(results.failedTestIds().isEmpty());
    }

    @Test
    void missingDirectoryYieldsEmptyResults() throws Exception {
        SurefireExecutor.SurefireTestResults results =
                SurefireExecutor.parseSurefireReports(tempDir.resolve("does-not-exist"));

        assertTrue(results.executedTestIds().isEmpty());
        assertTrue(results.failedTestIds().isEmpty());
    }
}
