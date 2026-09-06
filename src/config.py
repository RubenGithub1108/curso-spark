"""Configuración centralizada y robusta de SparkSession para Windows y Conda."""

import os
import sys
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# 1. Forzar a Spark y sus workers a usar el Python de Conda activo
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 2. Garantizar que Java de Conda sea detectado
if "JAVA_HOME" not in os.environ:
    conda_java = os.path.join(sys.prefix, "Library")
    if os.path.exists(os.path.join(conda_java, "bin", "java.exe")):
        os.environ["JAVA_HOME"] = conda_java

# 3. Configurar soporte nativo de Hadoop en Windows
HADOOP_HOME = r"C:\hadoop"
if os.name == "nt":
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["hadoop.home.dir"] = HADOOP_HOME
    bin_path = os.path.join(HADOOP_HOME, "bin")
    if bin_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_path + os.pathsep + os.environ.get("PATH", "")


def get_spark_session(app_name: str = "ModernPySparkMasterclass") -> SparkSession:
    """Crea una SparkSession estable con Delta Lake, Arrow y AQE."""
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()