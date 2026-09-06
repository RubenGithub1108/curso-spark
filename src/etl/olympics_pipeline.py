"""Logica de transformacion del dataset de olimpiadas."""
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

DEPORTISTAS_SCHEMA = StructType([
    StructField("deportista_id", IntegerType(), False),
    StructField("nombre", StringType(), False),
    StructField("genero", IntegerType(), True),
    StructField("edad", IntegerType(), True),
    StructField("altura", DoubleType(), True),
    StructField("peso", DoubleType(), True),
    StructField("equipo_id", IntegerType(), True),
])

def calcular_imc(df: DataFrame) -> DataFrame:
    return df.withColumn("imc", F.when(
        (F.col("altura") > 0) & F.col("peso").isNotNull(),
        F.round(F.col("peso") / ((F.col("altura") / 100.0) ** 2), 2)
    ).otherwise(None))

def clasificar_imc_categoria(df: DataFrame) -> DataFrame:
    return df.withColumn("categoria_imc", F.when(F.col("imc") < 18.5, "Bajo Peso")
        .when((F.col("imc") >= 18.5) & (F.col("imc") < 25.0), "Normal")
        .when((F.col("imc") >= 25.0) & (F.col("imc") < 30.0), "Sobrepeso")
        .when(F.col("imc") >= 30.0, "Obesidad")
        .otherwise("Desconocido"))
