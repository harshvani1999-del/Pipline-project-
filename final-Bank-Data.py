# Databricks notebook source
# 1. IMPORTS & SPARK SESSION
# ============================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, countDistinct, max, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

# 1. Load Raw Data (Dummy CSV from DBFS)
file_path = "/Volumes/data-bank/default/data/file/banking_data.csv"
raw_df = spark.read.csv(file_path, header=True, inferSchema=True)
print("Raw Data Sample:")
display(raw_df.limit(5))

# COMMAND ----------

# 3. STAGING LAYER (CLEANING)
# ============================
staging_df = (raw_df.dropDuplicates()
              .na.drop()
              .withColumn("amount", col("amount").cast("double"))
              .withColumn("transaction_date", col("transaction_date").cast("date"))
              .withColumn("ingestion_date", current_timestamp()))  # Audit column

print("Staging Data Sample:")
display(staging_df.limit(5))

# COMMAND ----------

# 4. DATA VALIDATION
# ============================

# Example validation: negative amounts should not exist
invalid_df = staging_df.filter(col("amount") < 0)
if invalid_df.count() > 0:
    raise ValueError("Invalid records found with negative amounts!")


# COMMAND ----------

# Select only columns that exist in the target table
columns = [
    "transaction_id",
    "customer_id",
    "transaction_date",
    "transaction_type",
    "amount",
    "branch"
]

staging_df.select(columns).write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("banking_staging")

print("✅ Staging table created successfully")

# COMMAND ----------

# 6. REPORTING LAYER
# ============================

reporting_df = (staging_df.groupBy("customer_id")
                .agg(
                    sum("amount").alias("total_balance"),
                    avg("amount").alias("avg_transaction"),
                    countDistinct("transaction_id").alias("transaction_count"),
                    max("transaction_date").alias("last_transaction_date")
                )
                .withColumn("report_generated_at", current_timestamp()))  # Audit column

reporting_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("banking_reporting")

print("✅ Reporting table created successfully")

# COMMAND ----------

# 7. UPSERT LOGIC (MERGE INTO)
# ============================

# Example: Naya transaction aya ya update hua → merge into staging
delta_table = DeltaTable.forName(spark, "banking_staging")

delta_table.alias("t").merge(
    staging_df.alias("s"),
    "t.transaction_id = s.transaction_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("✅ Merge (Upsert) operation done on staging table")

# COMMAND ----------

# 8. VERIFY OUTPUT
# ============================

print("Staging Table:")
display(spark.sql("SELECT * FROM banking_staging LIMIT 10"))

print("Reporting Table:")
display(spark.sql("SELECT * FROM banking_reporting LIMIT 10"))

# COMMAND ----------

