# Databricks notebook source
sales_org_cluster = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Sales_Org_Cluster.csv")

# COMMAND ----------

Sales_Org_TL = spark.read.format("csv").option("header", "true").options(inferSchema=True).load("dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Sales_Org_TL.csv")

# COMMAND ----------

Sales_Org_TSE = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Sales_Org_TSE.csv")

# COMMAND ----------

GLAccountMaster = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/balajid@systechusa.com/GLAccountMaster.csv")

# COMMAND ----------

GLAccountMaster.display()

# COMMAND ----------

Sales_Org_TSE.display()

# COMMAND ----------

Sales_Org_TL.display()

# COMMAND ----------

sales_org_cluster.display()

# COMMAND ----------

import uuid
from pyspark.sql.types import StringType

# df_with_uuid = sales_org_cluster.withColumn("id", uuid())
def generate_uuid():
    return str(uuid.uuid4())

# Register the UDF (User-Defined Function)
generate_uuid_udf = udf(generate_uuid, StringType())

# Add a new column "id" with UUIDs
df_with_uuid = sales_org_cluster.withColumn("id", generate_uuid_udf())

df_with_uuid.display()



# COMMAND ----------

# MAGIC %pip install uuid
# MAGIC

# COMMAND ----------


