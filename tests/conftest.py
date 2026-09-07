"""Fixture de SparkSession ligera para ejecución de pruebas."""

import os
import sys
import pytest
from pyspark.sql import SparkSession

# Forzar intérprete activo para workers
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Instancia SparkSession mínima para pruebas unitarias rápidas."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("Pytest-Olympics-Suite")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()