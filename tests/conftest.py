"""Configuración global de Spark para Pytest en Windows."""

import os
import sys
import pytest
from pyspark.sql import SparkSession

# 1. Forzar a Spark a usar el Python exacto de Conda (evita el alias de Microsoft Store)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 2. Configurar soporte nativo de Hadoop en Windows
HADOOP_HOME = r"C:\hadoop"
if os.name == "nt":
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["hadoop.home.dir"] = HADOOP_HOME
    bin_path = os.path.join(HADOOP_HOME, "bin")
    if bin_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_path + os.pathsep + os.environ.get("PATH", "")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Instancia de SparkSession optimizada para pruebas locales."""
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    session = (
        SparkSession.builder.master("local[2]")
        .appName("Pytest-Spark-Suite")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()