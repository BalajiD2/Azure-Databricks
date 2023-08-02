# Databricks notebook source
# MAGIC %run /Users/balajid@systechusa.com/NB_SILVER

# COMMAND ----------

# DBTITLE 1,Adding Audit columns in Tables
def add_columns_with_defaults(input_df):
    # Get the current timestamp as Datetime
    current_timestamp = datetime.now()
    
    # Add the 'created_at' and 'created_by' columns with the current timestamp and constant value "IN1510"
    # 'created_at' will be of Datetime type
    input_df = input_df.withColumn("created_at", F.lit(current_timestamp)) \
                       .withColumn("created_by", F.lit("IN1506"))\
                           .withColumn("updated_at", F.lit(None).cast("timestamp"))


    return input_df

# COMMAND ----------

# DBTITLE 1,Audit columns Added
Alc_Company_Master = add_columns_with_defaults(Alc_Company_Master)
Alc_Customer_Master = add_columns_with_defaults(Alc_Customer_Master)
Alc_Plant_Master = add_columns_with_defaults(Alc_Plant_Master)
Alc_Outlet_Master = add_columns_with_defaults(Alc_Outlet_Master)
Alc_Product_Master = add_columns_with_defaults(Alc_Product_Master)
Alc_Competitor_Product_Master = add_columns_with_defaults(Alc_Competitor_Product_Master)

# COMMAND ----------

# DBTITLE 1,Generating Surrogate Keys
def generate_surrogate_key(df, surrogate_key):
    # Check if the surrogate_key column is already present in the DataFrame
    if surrogate_key not in df.columns:
        # Create a Window specification ordering by a constant column to establish the row order
        window_spec = Window.orderBy(lit(0))

        # Add a temporary column 'row_order' to represent the order of rows within each partition
        df_with_row_order = df.withColumn("row_order", lit(1))

        # Define a window specification ordering by the temporary column 'row_order'
        window_spec_order = Window.orderBy("row_order")

        # Generate consecutive surrogate keys for rows without an existing surrogate key
        df_with_surrogate_key = df_with_row_order.withColumn(surrogate_key, row_number().over(window_spec_order))

        # Drop the temporary column 'row_order' and reorder columns to have surrogate_key as the first column
        df_with_surrogate_key = df_with_surrogate_key.drop("row_order")
        columns = [surrogate_key] + [col_name for col_name in df_with_surrogate_key.columns if col_name != surrogate_key]
        df_with_surrogate_key = df_with_surrogate_key.select(columns)

    else:
        # Get the maximum value of the existing surrogate_key column
        max_key_row = df.agg(F.max(surrogate_key)).collect()[0]
        max_key = max_key_row[0] if max_key_row[0] is not None else 0

        # If the surrogate_key column is present, preserve the existing surrogate keys
        df_with_surrogate_key = df.withColumn(surrogate_key, when(col(surrogate_key).isNotNull(), col(surrogate_key))
                                                     .otherwise(row_number().over(Window.orderBy(lit(0))) + lit(max_key)))

        # Reorder columns to have surrogate_key as the first column
        columns = [surrogate_key] + [col_name for col_name in df_with_surrogate_key.columns if col_name != surrogate_key]
        df_with_surrogate_key = df_with_surrogate_key.select(columns)

    return df_with_surrogate_key

# COMMAND ----------

# DBTITLE 1,Adding Surrogate Keys to Dataframe
Alc_Company_Master 				= generate_surrogate_key(Alc_Company_Master,'Company_Master_key')
Alc_Company_Master 				= generate_surrogate_key(Alc_Company_Master,'Company_Master_key')
Alc_Customer_Master 			= generate_surrogate_key(Alc_Customer_Master,'Customer_Master_key')
Alc_Activation_Mapping 			= generate_surrogate_key(Alc_Activation_Mapping,'Activation_Mapping_key')
Alc_Activation_Master 			= generate_surrogate_key(Alc_Activation_Master,'Activation_Master_key')
Alc_Geography_Master     		= generate_surrogate_key(Alc_Geography_Master,'Geography_Master_key')
Alc_Outlet_Master      			= generate_surrogate_key(Alc_Outlet_Master,'Outlet_Master_key')
Alc_Plant_Master     			= generate_surrogate_key(Alc_Plant_Master,'Plant_Master_key')
Alc_Product_Master        		= generate_surrogate_key(Alc_Product_Master,'Product_Master_key')
Alc_Competitor_Product_Master   = generate_surrogate_key(Alc_Competitor_Product_Master,'Competitor_Product_Master_key')
Alc_Primary_Sales_Actuals       = generate_surrogate_key(Alc_Primary_Sales_Actuals,'Primary_Sales_Actuals_key')
Alc_Primary_Sales_Plan_AOP 		= generate_surrogate_key(Alc_Primary_Sales_Plan_AOP,'Primary_Sales_Plan_AOP_key')
Alc_Sales_Org_TL 				= generate_surrogate_key(Alc_Sales_Org_TL,'Sales_Org_TL_key')
Alc_Sales_Org_TSE 				= generate_surrogate_key(Alc_Sales_Org_TSE,'Sales_Org_TSE_key')
Alc_GL_Account_Master 			= generate_surrogate_key(Alc_GL_Account_Master,'GL_Account_Master_key')
Alc_Sales_Org_Cluster 			= generate_surrogate_key(Alc_Sales_Org_Cluster,'Sales_Org_Cluster_key')

# COMMAND ----------

# DBTITLE 1,Control Table Logic
def update_control_table(control_table_df, table_name, load_start_time, load_end_time, triggered_by, status, next_run, records_processed):
    new_record = [(table_name, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)]
    new_record_df = control_table_df.sql_ctx.createDataFrame(new_record,
                                                            ["Table_Name", "Load_Start_Datetime", "Load_End_Datetime",
                                                            "Triggered_By", "Status", "Next_Run", "Records_Processed"])
    control_table_df = control_table_df.union(new_record_df)

    return control_table_df



# COMMAND ----------

# DBTITLE 1,Control Table Dataframe
data = []
control_table_df = spark.createDataFrame(data, control_table_schema)

# COMMAND ----------

# DBTITLE 1,Truncate and load into Target Table
def trunc_and_load(table_name, source_df, schema):
    # Get the current timestamp as the load start time
    load_start_time = datetime.now()
    try:
        # Check if the table exists in the catalog
        if spark.catalog.tableExists(table_name):
            # Truncate the target table using SQL DROP TABLE statement
            target_df = spark.read.table(table_name)
            old_count = target_df.count()
            spark.sql(f"TRUNCATE TABLE {table_name}")
            print(f"Table '{table_name}' truncated.")
            
        else:
            print(f"Table '{table_name}' does not exist.")
        # Write the DataFrame with the specified schema to the target table
        print(old_count)
        df_with_schema = spark.createDataFrame(source_df.rdd, schema)
        df_with_schema.write.mode("overwrite").saveAsTable(table_name)
        print(f"Data loaded to Delta table '{table_name}'.")
        # Get the current timestamp as the load end time
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = 'Completed'
        next_run = datetime.now() + timedelta(days=1)
        records_processed = df_with_schema.count() - old_count
    except Exception as e:
        print("Error:", str(e))
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = "Failed"
        next_run = None
        records_processed = 0

    # Create a DataFrame for the updated control table
    control_table_data = [(table_name, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)]
    control_table_columns = ["Table_Name", "Load_Start_Datetime", "Load_End_Datetime", "Triggered_By", "Status", "Next_Run", "Records_Processed"]
    updated_control_table_df = spark.createDataFrame(control_table_data, control_table_columns)

    # Show the updated control table DataFrame
    updated_control_table_df.show()

    # Write the updated control table DataFrame to the control_table Delta table
    updated_control_table_df.write.mode("append").saveAsTable("IN1506.control_table")
    # updated_control_table_df.write.option("delta.enableChangeDataFeed", "true").option("delta.columnMapping.mode", "name").saveAsTable("IN1506.control_table")

    return updated_control_table_df

# COMMAND ----------

# DBTITLE 1,Truncate and load
trunc_and_load("IN1506.Alc_Activation_Mapping_IN1506",Alc_Activation_Mapping,ActivationMapping_Schema)
trunc_and_load("IN1506.Alc_Activation_Master_IN1506",Alc_Activation_Master,ActivationMaster_Schema)
trunc_and_load("IN1506.Alc_Geography_Master_IN1506",Alc_Geography_Master,GeographyMaster_Schema)
trunc_and_load("IN1506.Alc_GL_Account_Master_IN1506",Alc_GL_Account_Master,GLAccountMaster_Schema)
trunc_and_load("IN1506.Alc_Sales_Org_Cluster_IN1506",Alc_Sales_Org_Cluster,Sales_Org_Cluster_Schema)
trunc_and_load("IN1506.Alc_Sales_Org_TL_IN1506",Alc_Sales_Org_TL,Sales_Org_TL_Schema)
trunc_and_load("IN1506.Alc_Sales_Org_TSE_IN1506",Alc_Sales_Org_TSE,Sales_Org_TSE_Schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IN1506.control_table

# COMMAND ----------

# DBTITLE 1,Delete and load
def del_and_load(df,col_name, grain_columns,target_table,schema):
    load_start_time = datetime.now()
    try:
        # Check if the table exists in the catalog
        if spark.catalog.tableExists(target_table):
            # Truncate the target table using SQL DROP TABLE statement
            target_df = spark.read.table(target_table)
            old_count = target_df.count()
            latest_month = df.selectExpr(f"MAX({col_name}) as max_month").collect()[0]["max_month"]
            condition = F.col(col_name) != latest_month
            for col in grain_columns:
                print(col)
                condition |= (F.col(col) != F.lit(df.select(col).first()[0]))
                print(condition)
            filtered_df = df.filter(condition)
            filter_df = filtered_df.filter(F.date_format(col_name, "yyyy-MM-dd") != lit(latest_month))
        # spark.sql(f"DELETE TABLE {table_name} WHERE {col_name} = max(col_name)")
            print(f"Latest Record from the source dataframe '{df}' Delted.")

            df_with_schema = spark.createDataFrame(filter_df.rdd, schema)
# Step 2: Filter the DataFrame to exclude records with the latest month
            df_with_schema.write.format("Delta").mode("overwrite").saveAsTable(target_table)
            print(f"Data loaded to Delta table '{target_table}'.")
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = 'Completed'
        next_run = datetime.now() + timedelta(days=1)
        records_processed = df_with_schema.count() - old_count

    except Exception as e:
        print("Error:", str(e))
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = "Failed"
        next_run = None
        records_processed = 0

    control_table_data = [(target_table, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)]
    control_table_columns = ["Table_Name", "Load_Start_Datetime", "Load_End_Datetime", "Triggered_By", "Status", "Next_Run", "Records_Processed"]
    updated_control_table_df = spark.createDataFrame(control_table_data, control_table_columns)

    # Show the updated control table DataFrame
    updated_control_table_df.show()

    # Write the updated control table DataFrame to the control_table Delta table
    updated_control_table_df.write.mode("append").saveAsTable("IN1506.control_table")
    # updated_control_table_df.write.option("delta.enableChangeDataFeed", "true").option("delta.columnMapping.mode", "name").saveAsTable("IN1506.control_table")

    return updated_control_table_df

# COMMAND ----------

# DBTITLE 1,Delete and load
del_load = del_and_load(Alc_Primary_Sales_Plan_AOP,'mon',['skucode'], 'IN1506.Alc_Primary_Sales_Plan_AOP_IN1506', Primary_Sales_Plan_AOP_Schema)

# COMMAND ----------

# DBTITLE 1,Creating Tables
def df_with_schema(df, table_name,schema):
    # Add columns with defaults to the DataFrame
    # df_schema = df.schema
    # df_final = spark.createDataFrame(df.rdd, df_schema)
    # df_final = spark.createDataFrame(df.rdd, schema)
    
    # Check if the table exists in the catalog and drop it if it does
    if spark.catalog.tableExists(f"IN1506.{table_name}"):
        spark.sql(f"DROP TABLE IF EXISTS IN1506.{table_name}")
        print(f"Table IN1506.{table_name} dropped.")
    
    # Save the DataFrame as a Delta table
    df_final.write.option("delta.enableChangeDataFeed", "true").option("delta.columnMapping.mode", "name").saveAsTable(f"IN1506.{table_name}")
    print(f"Data loaded to Delta table IN1506.{table_name}.")
    
    return df_final

# COMMAND ----------

# DBTITLE 1,Target table loading
# df_with_schema(Alc_Company_Master,"IN1506.Alc_Company_Master",CompanyMaster_Schema)
# df_with_schema(Alc_Customer_Master,"IN1506.Alc_Customer_Master_",CustomerMaster_Schema)
# df_with_schema(Alc_Outlet_Master,"IN1506.Alc_Outlet_Master_",OutletMaster_Schema)
# df_with_schema(Alc_Plant_Master,"IN1506.Alc_Plant_Master",PlantMaster_Schema)
# df_with_schema(Alc_Product_Master,"IN1506.Alc_Product_Master",ProductMaster_Schema)
# df_with_schema(Alc_Competitor_Product_Master,"IN1506.Alc_Competitor_Product_Master",CompetitorProductMaster_Schema)

# COMMAND ----------

def convert_to_delta(df, table_name):
    # Save the DataFrame as a Delta table with the provided table name
    delta_table_path = f'dbfs:/user/hive/warehouse/IN1506/IN1506.{table_name}_IN1506'
    df.write.format("delta").mode("overwrite").option('delta.columnMapping.mode' , 'name').save(delta_table_path)

    print(f"DataFrame has been converted and stored as a Delta table: {table_name}")

# COMMAND ----------

# DBTITLE 1,Joining Keys For Fact Tables
# Perform the joins and select columns
Alc_Primary_Sales_Actuals = Alc_Primary_Sales_Actuals.alias('a') \
.join(Alc_Geography_Master.alias('b'), (col('a.statecode') == col('b.statecode')) & (col('a.countrycode') == col('b.countrycode')), how="left") \
.join(Alc_Company_Master.alias('c'), col('a.companycode') == col('c.companycode'), how="left") \
.join(Alc_Customer_Master.alias('d'), col('a.customercode') == col('d.customercode'), how="left") \
.join(Alc_Product_Master.alias('e'), col('a.skucode') == col('e.skucode'), how="left") \
.join(Alc_Plant_Master.alias('f'), col('a.plantcode') == col('f.plantcode'), how="left") \
.select(
col('b.geography_master_key').alias('geography_master_key'),
col('c.company_master_key').alias('company_master_key'),
col('d.customer_master_key').alias('customer_master_key'),
col('e.product_master_key').alias('product_master_key'),
col('f.plant_master_key').alias('plant_master_key'),
Alc_Primary_Sales_Actuals['*']
)

# Add a new column "created_timestamp" with the current timestamp
Alc_Primary_Sales_Actuals = Alc_Primary_Sales_Actuals.withColumn("created_timestamp", current_timestamp())

# Display the DataFrame
Alc_Primary_Sales_Actuals.display()

# Write the DataFrame to a Delta table in overwrite mode
Alc_Primary_Sales_Actuals.write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed", "true").option("delta.columnMapping.mode", "name").saveAsTable("IN1506.Alc_Primary_Sales_Actuals_IN106")

# COMMAND ----------

# DBTITLE 1,Incremental Load
def Incremental_load(source_df, target_table):
    load_start_time = datetime.now()
    df_final = spark.createDataFrame([], source_df.schema) 
    old_count = 0     
    try:
        # Check if the table exists in the catalog
        if spark.catalog.tableExists(target_table):
            # Truncate the target table using SQL DROP TABLE statement
                target_table_df = spark.read.table(f"IN1506.{target_table}")
                max_date = target_table_df.agg({'transactiondate': 'max'}).collect()[0][0]

                # Filter the new data based on the maximum transaction date from the target table
                df_final = source_df.where(col("transactiondate") > max_date)

                # Save the filtered data to the target table
                df_final.write.mode('append').saveAsTable(f"IN1506.{target_table}")
                        
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = 'Completed'
        next_run = datetime.now() + timedelta(days=1)
        records_processed = df_final.count() - old_count
    except Exception as e:
        print("Error:", str(e))
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = "Failed"
        next_run = None
        records_processed = 0
    control_table_data = [(target_table, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)]
    control_table_columns = ["Table_Name", "Load_Start_Datetime", "Load_End_Datetime", "Triggered_By", "Status", "Next_Run", "Records_Processed"]
    updated_control_table_df = spark.createDataFrame(control_table_data, control_table_columns)

    # Show the updated control table DataFrame
    updated_control_table_df.show()

    # Write the updated control table DataFrame to the control_table Delta table
    updated_control_table_df.write.mode("append").saveAsTable("IN1506.control_table")

    return updated_control_table_df

Incremental_load(Alc_Primary_Sales_Actuals, 'Alc_Primary_Sales_Actuals_IN1506')


# COMMAND ----------

def SCD_1(source_df, key_column, table_name, join_keys, scd_columns):
    load_start_time = datetime.now()
    old_count = 0 
    try:
        source_df_with_keys = []
        # Check if the table exists in the catalog
        if spark.catalog.tableExists(f'IN1506.{table_name}'):
            # Read the target DataFrame from the table (non-Delta format)
            target_df = spark.read.table(f'IN1506.{table_name}')
            # Convert the target DataFrame into a Delta table
            convert_to_delta(target_df, f'IN1506.{table_name}') 
            # Get the path for the Delta table
            delta_table_path = f'dbfs:/user/hive/warehouse/IN1510/IN1510.{table_name}'
            delta_table = DeltaTable.forPath(spark, delta_table_path)            
            old_count = target_df.count() 
            source_df_with_keys = generate_surrogate_key(source_df, key_column)
            # Perform the SCD1 merge operation using DeltaTable.merge
            delta_table.alias('target_table') \
                .merge(
                    source_df_with_keys.alias('source_table'),
                    ' AND '.join(f'target_table.{key} = source_table.{key}' for key in join_keys)
                ) \
                .whenMatchedUpdate(
                    condition=expr(' OR '.join(f'target_table.{col_name} != source_table.{col_name}' for col_name in scd_columns + join_keys)),
                    set={
                        # Update SCD columns with the new values for the rows that have changed
                        **{col_name: f'source_table.{col_name}' for col_name in scd_columns},
                        # Update 'updated_at' column with the current timestamp
                        'updated_at': current_timestamp()
                    }
                ) \
                .whenNotMatchedInsertAll() \
                .execute()
            print(f"SCD1 merge operation completed for table: {table_name}")
            # After the SCD1 merge operation, overwrite the target table with the updated data from the Delta table
            delta_table.toDF().write.mode("overwrite").format("delta").saveAsTable(f'IN1506.{table_name}') 
            print(f"Target table updated with the Delta table data for: {table_name}")
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = 'Completed'
        next_run = datetime.now() + timedelta(days=1)
        records_processed = source_df_with_keys.count() - old_count
    except Exception as e:
        print("Error:", str(e))
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = "Failed"
        next_run = None
        records_processed = 0
    control_table_data = [(table_name, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)]
    control_table_columns = ["Table_Name", "Load_Start_Datetime", "Load_End_Datetime", "Triggered_By", "Status", "Next_Run", "Records_Processed"]
    updated_control_table_df = spark.createDataFrame(control_table_data, control_table_columns)     # Show the updated control table DataFrame
    updated_control_table_df.show() 
    # Write the updated control table DataFrame to the control_table Delta table
    updated_control_table_df = update_control_table(updated_control_table_df, table_name, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)
    updated_control_table_df.write.mode("append").option("mergeSchema", "true").saveAsTable("IN1506.control_table")

    return updated_control_table_df

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IN1506.Alc_Plant_Master

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE IN1506.Alc_Plant_Master SET plantname = 'AO1111' WHERE 
# MAGIC plantcode = 'PL0001'

# COMMAND ----------

SCD_1(Alc_Company_Master,"alc_company_master_key", "Alc_Company_Master", ["companycode"], ["SalesOrgCode", "SalesOrgName"])
SCD_1(Alc_Plant_Master,"alc_plant_master_key", "Alc_Plant_Master", ["plantcode"], ["PlantName"])

# COMMAND ----------

# DBTITLE 1,SCD2
def SCD2(source_df, table_name,key_column , join_keys, scd_columns, start_date_col, end_date_col):
    load_start_time = datetime.now()
    df_final = spark.createDataFrame([], source_df.schema) 
    old_count = 0     
    try:
        # Get the path for the Delta table
        delta_table_path = f'dbfs:/user/hive/warehouse/IN1510/{table_name}'
        
        # Check if the Delta table already exists
        delta_table_exists = DeltaTable.isDeltaTable(spark, delta_table_path)

        if not delta_table_exists:
            # If the Delta table doesn't exist, convert the source DataFrame to a Delta table
            convert_to_delta(source_df, f'IN1506.{table_name}_IN1506')

        source_df_with_keys = generate_surrogate_key(source_df, key_column)
        # Add new columns 'start_date' and 'end_date' to the source DataFrame with the current timestamp
        source_df_with_dates = source_df.withColumn('start_date', lit(current_timestamp())) \
                                        .withColumn('end_date', lit('9999-12-31'))

        # Get the Delta table instance using an alias
        delta_table = DeltaTable.forName(spark, f'IN1506.{table_name}_IN1506')

        # Perform the SCD2 merge operation using DeltaTable.merge
        delta_table.alias('target_table') \
            .merge(
                source_df_with_dates.alias('source_table'),
                ' AND '.join(f'target_table.{key} = source_table.{key}' for key in join_keys)
            ) \
            .whenMatchedUpdate(
                condition=' OR '.join(f'target_table.{col_name} != source_table.{col_name}' for col_name in scd_columns),
                set={
                    'target_table.updated_at': 'source_table.start_date'
                }
            ) \
            .whenNotMatchedInsertAll() \
            .execute()

        print(f"SCD2 merge operation completed for table: {table_name}")

        # After the SCD2 merge operation, overwrite the target table with the updated data from the Delta table
        delta_table.toDF().write.mode("overwrite").option("mergerSchema",True).format("delta").saveAsTable(f'IN1506.{table_name}')

        print(f"Target table updated with the Delta table data for: {table_name}")

        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = 'Completed'
        next_run = datetime.now() + timedelta(days=1)
        records_processed = df_final.count() - old_count
    except Exception as e:
        print("Error:", str(e))
        load_end_time = datetime.now()
        triggered_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        status = "Failed"
        next_run = None
        records_processed = 0   
    control_table_data = [(table_name, load_start_time, load_end_time, triggered_by, status, next_run, records_processed)]
    control_table_columns = ["Table_Name", "Load_Start_Datetime", "Load_End_Datetime", "Triggered_By", "Status", "Next_Run", "Records_Processed"]
    updated_control_table_df = spark.createDataFrame(control_table_data, control_table_columns)

    # Show the updated control table DataFrame
    updated_control_table_df.show()

    # Write the updated control table DataFrame to the control_table Delta table
    updated_control_table_df.write.mode("append").saveAsTable("IN1506.control_table")

    return updated_control_table_df


# COMMAND ----------

SCD2(Alc_Outlet_Master, "Alc_Outlet_Master",'Outlet_Master_key', ["outlet_key","Outlet_Master_key"], ["outlet_status", "owner_code", "owner_name"], "created_at", "updated_at")

# COMMAND ----------

def calculate_age_in_days(created_at, updated_at):
    # Check if either 'updated_at' or 'created_at' is None
    if updated_at is None or created_at is None:
        return 0.0  # Return 0.0 as a double data type
    # Calculate age in days
    return (updated_at - created_at).days  # Return the age in days
def create_event_table(source_df, key_column, table_name, join_keys, scd_columns):  
    # Read the target DataFrame from the table
    target_df = spark.read.table(f'IN1506.{table_name}') 
    # Convert the 'companycode' column to lowercase in target_df and ensure the same data type
    for col_name in join_keys:
        target_df = target_df.withColumn(col_name, col(col_name).cast("string"))
        source_df = source_df.withColumn(col_name, source_df[col_name].cast("string")) 
    # Now perform the join
    join_condition = ' AND '.join(f'source_table.{key} = target_table.{key}' for key in join_keys)
    event_table_data = source_df.alias('source_table').join(
        target_df.alias('target_table'),
        expr(join_condition),
        'inner'
    ) 
    # Calculate age in days using a UDF
    calculate_age_udf = udf(calculate_age_in_days, DoubleType())  # Use DoubleType
    event_table_data = event_table_data.withColumn("Age", calculate_age_udf(col('target_table.created_at'), col('target_table.updated_at')))
   # Check for changes in SCD columns
    scd_column_changed = expr(' OR '.join(f'target_table.{col_name} != source_table.{col_name}' for col_name in scd_columns))
    event_table_data = event_table_data.where(scd_column_changed)
    event_table_data = event_table_data.select(
        col(f'target_table.{key_column}').alias('Surrogate_Key'),
        lit(table_name).alias('Table_Name'),
        lit(scd_columns[0]).alias('SCD_Column'),
        expr(f"concat_ws(', ', {', '.join(f'target_table.{col_name}' for col_name in scd_columns)})").alias('Records'),
        col('target_table.created_at').alias('Created_At'),
        col('target_table.updated_at').alias('Updated_At'),
        col('Age')
    ) 
    # Save the event table DataFrame to the Delta event table
    event_table_data.write \
        .option("mergeSchema", "true") \
        .mode("append") \
        .format("delta") \
        .saveAsTable("IN1506.event_table")


# COMMAND ----------

create_event_table(Alc_Company_Master, "company_master_key", "Alc_Company_Master", ["companycode"], ["SalesOrgCode", "SalesOrgName"])
create_event_table(Alc_Plant_Master,"plant_master_key", "Alc_Plant_Master", ["plantcode"], ["PlantName"])


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from IN1506.event_table
