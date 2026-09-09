package io.github.wpunit13.mutator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class TestContextTrackerTest {

    @Test
    void setThenGetReturnsSameValueOnSameThread() {
        try {
            TestContextTracker.setCurrentTestId("test-42");
            assertEquals("test-42", TestContextTracker.getCurrentTestIdOrNull());
        } finally {
            TestContextTracker.clearCurrentTestId();
        }
    }

    @Test
    void afterClearGetReturnsNull() {
        TestContextTracker.setCurrentTestId("test-42");
        TestContextTracker.clearCurrentTestId();
        assertNull(TestContextTracker.getCurrentTestIdOrNull());
    }

    @Test
    void valueIsNotVisibleFromOtherThreads() throws Exception {
        TestContextTracker.setCurrentTestId("main-thread-only");
        AtomicReference<String> observed = new AtomicReference<>("sentinel");
        Thread other = new Thread(() -> observed.set(TestContextTracker.getCurrentTestIdOrNull()));
        other.start();
        other.join();
        TestContextTracker.clearCurrentTestId();
        assertNull(observed.get(), "test id set on main thread must not leak into other threads");
    }
}
