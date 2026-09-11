package io.github.wpunit13.mutator

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import com.fasterxml.jackson.databind.ObjectMapper

/**
 * JVM-side fatal error diagnostic hook (Signal C diagnostics).
 *
 * Cross-cutting contract with Python session_driver.py:
 * When a fatal Error (OutOfMemoryError, StackOverflowError, LinkageError) occurs,
 * this hook writes a JSON sentinel file indicating the cause of the JVM termination.
 * On Py4JNetworkError, the Python harness reads this sentinel file to enrich
 * driver_crash failure details with the exact fatal exception class and message.
 *
 * Graceful-stop sentinel suppression: normal JVM termination does not write any
 * sentinel file; the hook only writes when a fatal Error is intercepted.
 */
object DriverFatalShutdownHook {

  val SENTINEL_PROP: String = "spark.mutator.sentinel.path"

  private val registered = new AtomicBoolean(false)
  private val mapper = new ObjectMapper()

  def register(): Unit = {
    if (registered.compareAndSet(false, true)) {
      try {
        val prevHandler = Thread.getDefaultUncaughtExceptionHandler
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
          override def uncaughtException(thread: Thread, t: Throwable): Unit = {
            if (isFatal(t)) {
              recordFatal(t)
            }
            if (prevHandler != null) {
              prevHandler.uncaughtException(thread, t)
            }
          }
        })
      } catch {
        case _: Throwable => // Best-effort: never throw during registration
      }
    }
  }

  def isFatal(t: Throwable): Boolean = t match {
    case _: OutOfMemoryError => true
    case _: StackOverflowError => true
    case _: LinkageError => true
    case _: VirtualMachineError => true
    case _ => false
  }

  /**
   * Pure, testable core: writes {"reason": "<simple class name>", "message": "...", "epochMillis": ...}
   * to the sentinel path configured via system property "spark.mutator.sentinel.path".
   * If unset or empty, this is a no-op and does not throw.
   */
  def recordFatal(t: Throwable): Unit = {
    val path = System.getProperty(SENTINEL_PROP)
    if (path != null && path.trim.nonEmpty) {
      try {
        val file = new File(path.trim)
        val parent = file.getParentFile
        if (parent != null && !parent.exists()) {
          parent.mkdirs()
        }
        val node = mapper.createObjectNode()
        node.put("reason", t.getClass.getSimpleName)
        node.put("message", if (t.getMessage != null) t.getMessage else "")
        node.put("epochMillis", System.currentTimeMillis())
        mapper.writeValue(file, node)
      } catch {
        case _: Throwable => // Best-effort: never throw
      }
    }
  }
}
