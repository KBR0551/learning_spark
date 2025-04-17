#incremental dataload -- identift new and modified record 
from pyspark.sql import SparkSession
from pyspark. sql. functions import col, max as spark_max
# Initialize Spark Session
spark = SparkSession.builder.appName("Incremental Parquet UPSERT"). getOrCreate()
# Define file paths
target_parquet_path =
"abfss://your_container@your_account.dfs.core.windows.net/output/target_data/"
source_csv_path =
"abfss://your_container@your_account.dfs.core.windows.net/input/new_data.csv"
# Load existing target data (Parquet)
target_df = spark. read. parquet(target_parquet_path)
# Load new incoming data (CSV)
source_df = spark. read.option ("header", "true"). csv(source_csv_path)
# Identify latest timestamp from target
latest_ts = target_df.select(spark_max("dw_mod_ts")).collect()[0][0]
# Filter new or updated records
new_changed_record = source_df. filter(col("dw_mod_ts") › latest_ts)
# Identify IDs of new/changed records
key_column = "id"
new_changed_id = new_changed_record.select(key_column) .distinct)
# Extract unchanged records from existing target
target_unchanged_df = target_df. join(new_changed_id, key_column, "left_anti")
# Merge updated/new records with unchanged data
final_df = target_unchanged_df.unionByName(new_changed_record)
# Write back to Parquet (overwrite mode)
final_df.write.mode("overwrite"). parquet (target_parquet_path)
