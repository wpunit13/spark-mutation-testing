"""Exception types raised by the Python side of spark-mutator."""


class UnsupportedSparkVersionError(Exception):
    """Raised when the running Spark/Scala combination has no built shim."""


class MutatorJvmError(Exception):
    """Raised when a JVM call made through the Py4J bridge fails.

    The originating Java exception's string representation is stored on
    :attr:`java_message`.
    """

    def __init__(self, message: str, java_message: str = "") -> None:
        super().__init__(message)
        self.java_message = java_message
