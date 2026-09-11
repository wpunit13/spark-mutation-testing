package io.github.wpunit13.mutator

import java.io.File
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite

class DriverFatalShutdownHookSpec extends AnyFunSuite {

  private val mapper = new ObjectMapper()

  test("recordFatal writes JSON containing reason and message to configured path") {
    val tempFile = File.createTempFile("driver-fatal-sentinel-", ".json")
    tempFile.deleteOnExit()

    try {
      System.setProperty(DriverFatalShutdownHook.SENTINEL_PROP, tempFile.getAbsolutePath)
      DriverFatalShutdownHook.recordFatal(new OutOfMemoryError("boom"))

      assert(tempFile.exists(), "sentinel file should have been written")
      val json = mapper.readTree(tempFile)
      assert(json.get("reason").asText() == "OutOfMemoryError")
      assert(json.get("message").asText() == "boom")
      assert(json.has("epochMillis"))
      assert(json.get("epochMillis").asLong() > 0)
    } finally {
      System.clearProperty(DriverFatalShutdownHook.SENTINEL_PROP)
      if (tempFile.exists()) {
        tempFile.delete()
      }
    }
  }

  test("recordFatal with sentinel path unset is a no-op and does not throw") {
    System.clearProperty(DriverFatalShutdownHook.SENTINEL_PROP)
    // With sentinel path unset, must be a no-op and must not throw
    DriverFatalShutdownHook.recordFatal(new OutOfMemoryError("boom"))
  }
}
