# Databricks notebook source
def read_tables(username,password,tablename):
    url = "jdbc:sqlserver://systechtraining.database.windows.net:1433;databaseName=trainer"
    df = (spark.read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", tablename)
    .option("user", username)
    .option("password", password)
    .load()
    )

    return df

# COMMAND ----------

list_tables = ['Alcobev.CompanyMaster'
,'Alcobev.CustomerMaster'
,'Alcobev.ActivationMapping'
,'Alcobev.ActivationMaster'
,'Alcobev.GeographyMaster'
,'Alcobev.OutletMaster'
,'Alcobev.PlantMaster'
,'Alcobev.ProductMaster'
,'Alcobev.CompetitorProductMaster'
,'Alcobev.Primary_Sales_Actuals'
,'Alcobev.Primary_Sales_Plan_AOP']

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Alcobev.CompanyMaster/_delta_log', True)

# COMMAND ----------

for i in list_tables:
    dbutils.fs.rm(f'dbfs:/FileStore/shared_uploads/balajid@systechusa.com/{i}_CSV', True)
    c = read_tables("brandmuscle_trainees","$yst3ch@!23",i)
    lower_cols=c.select([F.col(x).alias(x.lower()) for x in c.columns])
    lower_cols.write.mode('overwrite').format('csv').option("header", "true").save(f'dbfs:/FileStore/shared_uploads/balajid@systechusa.com/{i}_CSV')

# COMMAND ----------

df = spark.read.format('csv').option("header", "true").load('dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Alcobev.ActivationMaster')

# COMMAND ----------

df.display()

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/balajid@systechusa.com/GLAccountMaster.csv")
