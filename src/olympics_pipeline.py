"""Módulo con esquemas fuertemente tipados y transformaciones puras."""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

DEPORTISTAS_SCHEMA = StructType([
    StructField("deportista_id", IntegerType(), False),
    StructField("nombre", StringType(), False),
    StructField("genero", IntegerType(), True),
    StructField("edad", IntegerType(), True),
    StructField("altura", DoubleType(), True),
    StructField("peso", DoubleType(), True),
    StructField("equipo_id", IntegerType(), True),
])

EQUIPOS_SCHEMA = StructType([
    StructField("equipo_id", IntegerType(), False),
    StructField("equipo", StringType(), False),
    StructField("sigla", StringType(), True),
])

RESULTADOS_SCHEMA = StructType([
    StructField("resultado_id", IntegerType(), False),
    StructField("deportista_id", IntegerType(), False),
    StructField("evento_id", IntegerType(), False),
    StructField("medalla", StringType(), False),
])


def calcular_imc(df: DataFrame) -> DataFrame:
    """Calcula el Índice de Masa Corporal: peso(kg) / (altura(m) ** 2)."""
    return df.withColumn(
        "imc",
        F.when(
            (F.col("altura") > 0) & F.col("peso").isNotNull(),
            F.round(F.col("peso") / ((F.col("altura") / 100.0) ** 2), 2),
        ).otherwise(None),
    )


def clasificar_imc_categoria(df: DataFrame) -> DataFrame:
    """Clasifica a los atletas en categorías de IMC según la OMS."""
    return df.withColumn(
        "categoria_imc",
        F.when(F.col("imc") < 18.5, "Bajo Peso")
        .when((F.col("imc") >= 18.5) & (F.col("imc") < 25.0), "Normal")
        .when((F.col("imc") >= 25.0) & (F.col("imc") < 30.0), "Sobrepeso")
        .otherwise("Obesidad"),
    )


def agregar_medallero(df_dep: DataFrame, df_res: DataFrame) -> DataFrame:
    """Calcula el total de medallas obtenidas excluyendo registros 'NA'."""
    return (
        df_dep.join(df_res, on="deportista_id", how="inner")
        .filter(F.col("medalla") != "NA")
        .groupBy("deportista_id", "nombre")
        .agg(F.count("medalla").alias("total_medallas"))
    )