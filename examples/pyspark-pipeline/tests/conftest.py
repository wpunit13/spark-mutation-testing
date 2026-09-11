"""Shared fixtures for pipeline tests."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="session")
def spark():
    """Create or retrieve shared SparkSession for test execution."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("pyspark-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture
def customers_df(spark):
    """Customers fixture with 2 customer rows."""
    schema = StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), False),
        ]
    )
    data = [
        ("C1", "Alice"),
        ("C2", "Bob"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def orders_df(spark):
    """Orders fixture carefully engineered for survival/kill verification."""
    schema = StructType(
        [
            StructField("order_id", IntegerType(), False),
            StructField("customer_id", StringType(), False),
            StructField("amount", IntegerType(), False),
            StructField("status", StringType(), False),
        ]
    )
    data = [
        # Matched, satisfies both conjuncts (amount > 100 and status == "COMPLETED")
        (1, "C1", 150, "COMPLETED"),
        (2, "C2", 150, "COMPLETED"),
        # Matched, satisfies A, violates B (violates only 2nd conjunct: status != "COMPLETED")
        (3, "C1", 150, "PENDING"),
        # Matched, violates A, satisfies B (violates only 1st conjunct: amount <= 100)
        (4, "C2", 50, "COMPLETED"),
        # Unmatched orders, satisfy both conjuncts (satisfies A and B, unmatched customer_id)
        (5, "C99", 150, "COMPLETED"),
        (6, "C98", 150, "COMPLETED"),
    ]
    return spark.createDataFrame(data, schema)
