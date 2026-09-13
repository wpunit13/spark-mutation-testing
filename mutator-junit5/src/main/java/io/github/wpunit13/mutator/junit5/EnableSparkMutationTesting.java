package io.github.wpunit13.mutator.junit5;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Declarative annotation to activate spark-mutator mutation testing on JUnit 5 test classes.
 *
 * <p>When present on a test class, {@link SparkMutatorExtension} orchestrates the baseline
 * execution to discover mutation candidates from Catalyst plans, executes the test suite
 * against each discovered mutant, and reports mutation testing results.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@ExtendWith(SparkMutatorExtension.class)
public @interface EnableSparkMutationTesting {

    /**
     * Whether mutation testing is enabled for this test class. Defaults to {@code true}.
     * When {@code false}, {@link SparkMutatorExtension} behaves as a complete no-op.
     */
    boolean enabled() default true;
}
