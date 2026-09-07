"""Pruebas deterministas sobre el pipeline olímpico con Chispa."""

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType
from src.olympics_pipeline import agregar_medallero, calcular_imc, clasificar_imc_categoria


def test_calcular_imc(spark):
    schema_in = StructType([
        StructField("altura", DoubleType(), True),
        StructField("peso", DoubleType(), True),
    ])
    df_in = spark.createDataFrame([(180.0, 81.0), (None, 70.0), (170.0, None)], schema_in)
    df_out = calcular_imc(df_in)

    schema_exp = StructType(schema_in.fields + [StructField("imc", DoubleType(), True)])
    df_exp = spark.createDataFrame([
        (180.0, 81.0, 25.0),
        (None, 70.0, None),
        (170.0, None, None),
    ], schema_exp)

    assert_df_equality(df_out, df_exp, ignore_nullable=True)


def test_clasificar_imc_categoria(spark):
    schema_in = StructType([StructField("imc", DoubleType(), True)])
    df_in = spark.createDataFrame([(17.2,), (22.5,), (27.4,), (32.1,)], schema_in)
    df_out = clasificar_imc_categoria(df_in)

    schema_exp = StructType([
        StructField("imc", DoubleType(), True),
        StructField("categoria_imc", StringType(), False),
    ])
    df_exp = spark.createDataFrame([
        (17.2, "Bajo Peso"),
        (22.5, "Normal"),
        (27.4, "Sobrepeso"),
        (32.1, "Obesidad"),
    ], schema_exp)

    assert_df_equality(df_out, df_exp, ignore_nullable=True)


def test_agregar_medallero(spark):
    schema_dep = StructType([
        StructField("deportista_id", IntegerType(), False),
        StructField("nombre", StringType(), False),
    ])
    schema_res = StructType([
        StructField("deportista_id", IntegerType(), False),
        StructField("medalla", StringType(), False),
    ])

    df_dep = spark.createDataFrame([(1, "Alice"), (2, "Bob")], schema_dep)
    df_res = spark.createDataFrame([(1, "Gold"), (1, "Silver"), (2, "NA")], schema_res)

    df_out = agregar_medallero(df_dep, df_res)

    schema_exp = StructType([
        StructField("deportista_id", IntegerType(), False),
        StructField("nombre", StringType(), False),
        StructField("total_medallas", LongType(), False),
    ])
    df_exp = spark.createDataFrame([(1, "Alice", 2)], schema_exp)

    assert_df_equality(df_out, df_exp, ignore_nullable=True)