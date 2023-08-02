# Databricks notebook source
# DBTITLE 1,scd1
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def scd1(df, scd_columns, key_columns):
    """
    Function to perform Slowly Changing Dimension Type 1 (SCD1) on the DataFrame.

    Parameters:
    - df: The input DataFrame.
    - scd_columns: A list of column names that are subject to SCD changes.
    - key_columns: A list of key column names to uniquely identify records.

    Returns:
    - DataFrame with SCD1 updates.
    """
    # Get the current timestamp for updating the SCD columns
    current_timestamp = F.current_timestamp()
    
    # Create a DataFrame to store the latest records
    latest_records_df = df.select(*key_columns).distinct()

    # Select the latest records from the DataFrame
    latest_records = df.join(latest_records_df, on=key_columns, how='inner')
    
    # Select the columns that are subject to SCD changes from the original DataFrame
    scd_columns_df = df.select(*scd_columns)

    # Create a DataFrame containing the latest values of SCD columns
    latest_values_df = scd_columns_df.groupBy(key_columns).agg(*(F.first(col).alias(col) for col in scd_columns))

    # Join the latest values with the latest records DataFrame to update SCD columns
    updated_df = latest_records.join(latest_values_df, on=key_columns, how='left_outer')

    # Select all columns from the original DataFrame and the updated SCD columns
    updated_df = updated_df.select(df.columns + [F.coalesce(updated_df[col], df[col]).alias(col) for col in scd_columns])
    
    # Add the current timestamp to the SCD columns
    updated_df = updated_df.withColumn("valid_to", F.lit(None)).withColumn("valid_from", F.lit(None))
    updated_df = updated_df.withColumn("current_flag", F.lit(1)).withColumn("created_date", current_timestamp)

    # Return the final DataFrame with SCD1 updates
    return updated_df

# Example usage:
# Assuming you have a DataFrame 'df' and the columns 'customer_id' and 'name' are subject to SCD changes
# You can call the function as follows:
# updated_df = scd1(df, scd_columns=['name'], key_columns=['customer_id'])
# updated_df.show()


# COMMAND ----------

# DBTITLE 1,scd2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def scd2(df, scd_columns, key_columns):
    """
    Function to perform Slowly Changing Dimension Type 2 (SCD2) on the DataFrame.

    Parameters:
    - df: The input DataFrame.
    - scd_columns: A dictionary of column names that are subject to SCD changes along with their data types.
                   Example: {'name': 'string', 'age': 'integer'}
    - key_columns: A list of key column names to uniquely identify records.

    Returns:
    - DataFrame with SCD2 updates.
    """
    # Get the current timestamp for updating the SCD columns
    current_timestamp = F.current_timestamp()
    
    # Create a Window specification ordering by a constant column to establish the row order
    window_spec = Window.orderBy(F.lit(0))
    
    # Add a temporary column 'row_order' to represent the order of rows within each partition
    df_with_row_order = df.withColumn("row_order", F.when(F.count("*").over(window_spec) > 0, F.lit(1)))

    # Define a window specification ordering by the temporary column 'row_order'
    window_spec_order = Window.orderBy("row_order")

    # Add a surrogate key column
    df_with_surrogate_key = df_with_row_order.withColumn("surrogate_key", F.row_number().over(window_spec_order))
    
    # Select the columns that are subject to SCD changes from the original DataFrame
    scd_columns_df = df_with_surrogate_key.select(*scd_columns.keys())

    # Create a DataFrame containing the latest values of SCD columns
    latest_values_df = scd_columns_df.groupBy(key_columns).agg(*(F.first(col).alias(col) for col in scd_columns.keys()))

    # Join the latest values with the DataFrame with the surrogate key to update SCD columns
    updated_df = df_with_surrogate_key.join(latest_values_df, on=key_columns, how='left_outer')

    # Select all columns from the original DataFrame and the updated SCD columns
    updated_df = updated_df.select(df_with_surrogate_key.columns + 
                                   [F.coalesce(updated_df[col], df_with_surrogate_key[col]).alias(col) for col in scd_columns.keys()])

    # Add the current timestamp to the SCD columns
    for col, data_type in scd_columns.items():
        if data_type == 'string':
            default_value = F.lit("")
        elif data_type == 'integer':
            default_value = F.lit(0)
        elif data_type == 'date':
            default_value = F.lit("1970-01-01")  # You can change the default date as per your requirement
        else:
            default_value = F.lit(None)
        
        updated_df = updated_df.withColumn(f"{col}_valid_to", F.lit(None)) \
                               .withColumn(f"{col}_valid_from", F.lit(None)) \
                               .withColumn(f"{col}_current_flag", F.when(F.col(col) == updated_df[col], F.lit(1)).otherwise(F.lit(0))) \
                               .withColumn(f"{col}_created_date", F.when(F.col(col) == updated_df[col], current_timestamp).otherwise(default_value))

    # Drop the temporary 'row_order' and surrogate key columns
    updated_df = updated_df.drop("row_order", "surrogate_key")

    # Return the final DataFrame with SCD2 updates
    return updated_df

# Example usage:
# Assuming you have a DataFrame 'df' and the columns 'name' and 'age' are subject to SCD changes
# You can call the function as follows:
# scd_columns = {'name': 'string', 'age': 'integer'}
# updated_df = scd2(df, scd_columns=scd_columns, key_columns=['customer_id'])
# updated_df.show()


# COMMAND ----------


