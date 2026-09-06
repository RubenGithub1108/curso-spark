from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType, StructField, DoubleType
from src.etl.olympics_pipeline import calcular_imc

def test_calcular_imc(spark):
    schema = StructType([
        StructField("altura", DoubleType(), True),
        StructField("peso", DoubleType(), True)
    ])
    df_in = spark.createDataFrame([(180.0, 81.0)], schema)
    df_res = calcular_imc(df_in)
    df_exp = spark.createDataFrame(
        [(180.0, 81.0, 25.0)],
        StructType(schema.fields + [StructField("imc", DoubleType(), True)])
    )
    assert_df_equality(df_res, df_exp, ignore_nullable=True)
