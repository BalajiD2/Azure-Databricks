# Databricks notebook source
# MAGIC %run /Users/balajid@systechusa.com/NB_INITALIZE

# COMMAND ----------

# DBTITLE 1,Raw Source Files in Delta Path
alc_company_master_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Alcobev.CompanyMaster_delta"
alc_customer_master_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Alcobev.CustomerMaster_delta"
alc_activation_mapping_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Alcobev.ActivationMapping_delta"
alc_activation_master_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Alcobev.ActivationMaster_delta"
alc_geography_master_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Alcobev.GeographyMaster_delta"
alc_outlet_master_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Alcobev.OutletMaster_delta"
alc_plant_master_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Alcobev.PlantMaster_delta"
alc_product_master_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Alcobev.ProductMaster_delta"
alc_competitor_product_master_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Alcobev.CompetitorProductMaster_delta"
alc_primary_sales_actuals_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Alcobev.Primary_Sales_Actuals_delta"
alc_primary_sales_plan_aop_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Alcobev.Primary_Sales_Plan_AOP_delta"
alc_sales_org_tl_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Sales_Org_TL_delta"
alc_sales_org_tse_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Sales_Org_TSE_delta"
alc_gl_account_master_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/GLAccountMaster_delta"
alc_sales_org_cluster_path = "dbfs:/FileStore/shared_uploads/balajid@systechusa.com/Sales_Org_Cluster_delta"

# COMMAND ----------

# DBTITLE 1,Reading Source from Delta path
Alc_Company_Master = spark.read.format("delta").load(alc_company_master_path)
Alc_Customer_Master = spark.read.format("delta").load(alc_customer_master_path)
Alc_Activation_Mapping = spark.read.format("delta").load(alc_activation_mapping_path)
Alc_Activation_Master = spark.read.format("delta").load(alc_activation_master_path)
Alc_Geography_Master = spark.read.format("delta").load(alc_geography_master_path)
Alc_Outlet_Master = spark.read.format("delta").load(alc_outlet_master_path)
Alc_Plant_Master = spark.read.format("delta").load(alc_plant_master_path)
Alc_Product_Master = spark.read.format("delta").load(alc_product_master_path)
Alc_Competitor_Product_Master = spark.read.format("delta").load(alc_competitor_product_master_path)
Alc_Primary_Sales_Actuals = spark.read.format("delta").load(alc_primary_sales_actuals_path)
Alc_Primary_Sales_Plan_AOP = spark.read.format("delta").load(alc_primary_sales_plan_aop_path)
Alc_Sales_Org_TL = spark.read.format("delta").load(alc_sales_org_tl_path)
Alc_Sales_Org_TSE = spark.read.format("delta").load(alc_sales_org_tse_path)
Alc_GL_Account_Master = spark.read.format("delta").load(alc_gl_account_master_path)
Alc_Sales_Org_Cluster = spark.read.format("delta").load(alc_sales_org_cluster_path)

# COMMAND ----------

# DBTITLE 1,Alc_Customer_Master DataType Conversion
Alc_Customer_Master = (
    Alc_Customer_Master.withColumn("Division", col("Division").cast(IntegerType()))
    .withColumn("CreditDays", col("CreditDays").cast(IntegerType()))
    .withColumn("PostCode", col("PostCode").cast(IntegerType()))
)

# COMMAND ----------

# DBTITLE 1,Alc_Activation_Mapping DataType Conversion
Alc_Activation_Mapping = (
    Alc_Activation_Mapping.withColumn("effectivefrom", to_date(col("EffectiveFrom"), "dd-MM-yyyy"))
    .withColumn("effectiveto", to_date(col("EffectiveTo"), "dd-MM-yyyy"))
    .withColumn("isactive", col("IsActive").cast(IntegerType()))
)

# COMMAND ----------

# DBTITLE 1,Alc_Activation_Master DataType Conversion
Alc_Activation_Master = Alc_Activation_Master.withColumn("effectivefrom", to_date(col("EffectiveFrom"), "dd-MM-yyyy"))\
    .withColumn("effectiveto", to_date(col("EffectiveTo"), "dd-MM-yyyy"))

# COMMAND ----------

# DBTITLE 1,Alc_Outlet_Master DataType Conversion
Alc_Outlet_Master = (
    Alc_Outlet_Master.withColumn("geo_key", col("geo_key").cast(IntegerType()))
    .withColumn("active_flag", col("ACTIVE_FLAG").cast(BooleanType()))
    .withColumn("outlet_key", col("OUTLET_KEY").cast(IntegerType()))
)

# COMMAND ----------

# DBTITLE 1,Alc_Plant_Master DataType Conversion
Alc_Plant_Master  = Alc_Plant_Master.withColumn("companycode", col("companycode").cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Alc_Product_Master DataType Conversion
Alc_Product_Master = (
    Alc_Product_Master.withColumn("skucode", col("skucode").cast(IntegerType()))
    .withColumn("caseconfig", col("caseconfig").cast(IntegerType()))
    .withColumn("conversionfactor", col("conversionfactor").cast(IntegerType()))
)

# COMMAND ----------

# DBTITLE 1,Alc_Competitor_Product_Master DataType Conversion
Alc_Competitor_Product_Master = Alc_Competitor_Product_Master.withColumn("skucode", col("skucode").cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Alc_Primary_Sales_Actuals DataType Conversion
Alc_Primary_Sales_Actuals = (
    Alc_Primary_Sales_Actuals.withColumn("distributionchannel", col("distributionchannel").cast(IntegerType()))
    .withColumn("division", col("division").cast(IntegerType()))
    .withColumn("skucode", col("skucode").cast(IntegerType()))
    .withColumn("transactiondate", to_date(col("transactiondate"), "dd-MM-yyyy"))
    .withColumn("volumeactualcase", col("volumeactualcase").cast(IntegerType()))
)

# COMMAND ----------

# DBTITLE 1,Alc_Sales_Org_TL DataType Conversion
Alc_Sales_Org_TL = (
    Alc_Sales_Org_TL.withColumn("tl_start_date", to_date(col("tl_start_date"), "dd-MM-yyyy"))
    .withColumn("tl_end_date", to_date(col("tl_end_date"), "dd-MM-yyyy"))
    .withColumn("tl_territory_id", col("tl_territory_id").cast(IntegerType()))
    .withColumn("min_market_working_with_tse",col("min_market_working_with_tse").cast(IntegerType()))
    .withColumn("market_working_norms", col("market_working_norms").cast(IntegerType()))
    .withColumn("active_flag", col("active_flag").cast(IntegerType()))
    .withColumn("tl_code", col("tl_code").cast(IntegerType()))
)

# COMMAND ----------

# DBTITLE 1,Alc_Sales_Org_TSE DataType Conversion
Alc_Sales_Org_TSE = (
    Alc_Sales_Org_TSE.withColumn("tse_territory_id", col("tse_territory_id").cast(IntegerType())    )
    .withColumn("tse_code", col("tse_code").cast(IntegerType()))
    .withColumn("tse_call_norm", col("tse_call_norm").cast(IntegerType()))
    .withColumn("active_flag", col("active_flag").cast(IntegerType()))
    .withColumn("tse_start_date", to_date(col("tse_start_date"), "dd-MM-yyyy"))
    .withColumn("tse_end_date", to_date(col("tse_end_date"), "dd-MM-yyyy"))
)

# COMMAND ----------

# DBTITLE 1,Alc_GL_Account_Master DataType Conversion
Alc_GL_Account_Master = Alc_GL_Account_Master\
                        .withColumn("generalledgercode", col("generalledgercode").cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Alc_Sales_Org_Cluster DataType Conversion
Alc_Sales_Org_Cluster = (
    Alc_Sales_Org_Cluster.withColumn("cluster_start_date", to_date(col("cluster_start_date"), "dd-MM-yyyy")    )
    .withColumn("cluster_end_date", to_date(col("cluster_end_date"), "dd-MM-yyyy"))
    .withColumn("cluster_id", col("cluster_id").cast(IntegerType()))
    .withColumn("active_flag", col("active_flag").cast(IntegerType()))
)

# COMMAND ----------

Alc_Primary_Sales_Plan_AOP = (
    Alc_Primary_Sales_Plan_AOP.withColumn("skucode", col("skucode").cast(IntegerType()))\
        .withColumn("planqty", col("plan qty").cast(FloatType()))
)

Alc_Primary_Sales_Plan_AOP = Alc_Primary_Sales_Plan_AOP.drop("plan qty")

Alc_Primary_Sales_Plan_AOP = Alc_Primary_Sales_Plan_AOP.withColumn("mon", F.to_date(F.concat(F.substring("month", 1, 3), F.lit("-"), F.substring("month", 5, 2), F.lit("-01")), "MMM-yy-dd"))
latest_month = Alc_Primary_Sales_Plan_AOP.select(F.max("month")).collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Null Handelling
def handle_nulls(df, table_type):
    default_values = {
        StringType(): "UNK",
        DateType(): datetime(2999, 12, 31),  # Use Python datetime for DateType
        IntegerType(): -1 if table_type == "dim" else 0,
        LongType(): 0,
        DoubleType(): 0
    }

    for col in df.columns:
        column_data_type = df.schema[col].dataType
        default_value = default_values.get(column_data_type, None)

        # Add handling for 'NULL' values
        df = df.withColumn(
            col,
            F.when(
                (df[col].isNull()) | (df[col] == "NULL"),
                default_value  # Use Python datetime for DateType
            ).otherwise(df[col])
        )

    return df

# COMMAND ----------

Alc_Company_Master = handle_nulls(Alc_Company_Master, 'Dim')
Alc_Customer_Master = handle_nulls(Alc_Customer_Master, 'Dim')
Alc_Activation_Mapping = handle_nulls(Alc_Activation_Mapping, 'Dim')
Alc_Activation_Master = handle_nulls(Alc_Activation_Master, 'Dim')
Alc_Geography_Master = handle_nulls(Alc_Geography_Master, 'Dim')
Alc_Outlet_Master = handle_nulls(Alc_Outlet_Master, 'Dim')
Alc_Plant_Master = handle_nulls(Alc_Plant_Master, 'Dim')
Alc_Product_Master = handle_nulls(Alc_Product_Master, 'Dim')
Alc_Competitor_Product_Master = handle_nulls(Alc_Competitor_Product_Master, 'Dim')
Alc_Sales_Org_TL = handle_nulls(Alc_Sales_Org_TL, 'Dim')
Alc_Sales_Org_TSE = handle_nulls(Alc_Sales_Org_TSE, 'Dim')
Alc_GL_Account_Master = handle_nulls(Alc_GL_Account_Master, 'Dim')
Alc_Sales_Org_Cluster = handle_nulls(Alc_Sales_Org_Cluster, 'Dim')
Alc_Primary_Sales_Actuals = handle_nulls(Alc_Primary_Sales_Actuals, 'Fact')
Alc_Primary_Sales_Plan_AOP = handle_nulls(Alc_Primary_Sales_Plan_AOP, 'Fact')

# COMMAND ----------

# Alc_Company_Master.display()			
# Alc_Customer_Master.display() 			
# Alc_Activation_Mapping.display() 			
# Alc_Activation_Master.display()
# Alc_Geography_Master.display()
# Alc_Outlet_Master.display()
# Alc_Plant_Master.display()	
# Alc_Product_Master.display()		
# Alc_Competitor_Product_Master.display()
# Alc_Primary_Sales_Actuals.display()
# Alc_Primary_Sales_Plan_AOP.display()
# Alc_Sales_Org_TL.display()
# Alc_Sales_Org_TSE.display()	
# Alc_GL_Account_Master.display()
# Alc_Sales_Org_Cluster.display()
