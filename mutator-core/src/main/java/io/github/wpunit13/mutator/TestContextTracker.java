package io.github.wpunit13.mutator;

/**
 * Per-thread tracking of the test id currently under evaluation.
 *
 * <p>The value set on the driver thread that launches a Spark action is
 * deliberately not visible to other threads; each thread sees only what it
 * set itself.
 */
public final class TestContextTracker {

    private static final ThreadLocal<String> CURRENT_TEST_ID = new ThreadLocal<>();

    private TestContextTracker() {
    }

    public static void setCurrentTestId(String testId) {
        CURRENT_TEST_ID.set(testId);
    }

    /**
     * Clears the current test id. Uses {@link ThreadLocal#remove()} rather
     * than {@code set(null)} to avoid retaining thread-local entries in
     * pooled threads.
     */
    public static void clearCurrentTestId() {
        CURRENT_TEST_ID.remove();
    }

    /**
     * @return the test id set on the calling thread, or {@code null} if none.
     */
    public static String getCurrentTestIdOrNull() {
        return CURRENT_TEST_ID.get();
    }
}
