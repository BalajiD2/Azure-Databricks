# Databricks notebook source
# MAGIC %run /Users/balajid@systechusa.com/Schema_Defintion

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, DateType, TimestampType

def handle_nulls(df, table_type):
    schema = df.schema
    for col in df.columns:
        column_data_type = schema[col].dataType
        if table_type == "dim":
            if isinstance(column_data_type, StringType):
                df = df.withColumn(col, F.coalesce(df[col], F.lit("UNK")))
            elif isinstance(column_data_type, DateType):
                df = df.withColumn(col, F.coalesce(df[col], F.lit("2999-12-31")))
            elif isinstance(column_data_type, IntegerType):
                df = df.withColumn(col, F.coalesce(df[col], F.lit(-1)))
            elif isinstance(column_data_type, LongType):
                df = df.withColumn(col, F.coalesce(df[col], F.lit(-1)))
            elif isinstance(column_data_type, DoubleType):
                df = df.withColumn(col, F.coalesce(df[col], F.lit(-1.0)))
        elif table_type == "fact":
            if isinstance(column_data_type, StringType):
                df = df.withColumn(col, F.coalesce(df[col], F.lit("UNK")))
            elif isinstance(column_data_type, DateType):
                df = df.withColumn(col, F.coalesce(df[col], F.lit("2999-12-31")))
            elif isinstance(column_data_type, IntegerType):
                df = df.withColumn(col, F.coalesce(df[col], F.lit(0)))
            elif isinstance(column_data_type, LongType):
                df = df.withColumn(col, F.coalesce(df[col], F.lit(0)))
            elif isinstance(column_data_type, DoubleType):
                df = df.withColumn(col, F.coalesce(df[col], F.lit(0.0)))

    return df

#Dim
df_dim_with_null_handling = handle_nulls(Alc_Company_Master, table_type="dim")
df_dim_with_null_handling.display()
#Fact
# df_fact_with_null_handling = handle_nulls(df, table_type="fact")
# df_fact_with_null_handling.show()
