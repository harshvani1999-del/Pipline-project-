# Databricks notebook source
# DBTITLE 1,Step 1: Create Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MaterialRejectionAnalysis").getOrCreate()



# COMMAND ----------

# DBTITLE 1,Read CSV files

# Read CSVs
master_df = spark.read.option("header", True).csv("/Volumes/projectsworks/default/data/material_master.csv")
rejection_df = spark.read.option("header", True).csv("/Volumes/projectsworks/default/data/material_rejection1.csv")

display(master_df)
display(rejection_df)

# COMMAND ----------

# DBTITLE 1,Join datasets
joined_df = rejection_df.join(master_df, on="material_id", how="inner")



# COMMAND ----------

# DBTITLE 1,Select required columns
joined_df.select("rejection_id", "material_name", "rejection_reason", "quantity", "plant_location").show()

# COMMAND ----------

# DBTITLE 1,Find top rejection reasons
from pyspark.sql.functions import col, sum

top_reasons = joined_df.groupBy("rejection_reason") \
                      .agg(sum("quantity").alias("total_rejected")) \
                      .orderBy(col("total_rejected").desc())

top_reasons.show(3)


# COMMAND ----------

# DBTITLE 1,Save final data to Hive table
joined_df.write.mode("overwrite").saveAsTable("smith_material_rejection")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT rejection_reason, SUM(quantity) AS total_rejected
# MAGIC FROM smith_material_rejection
# MAGIC GROUP BY rejection_reason
# MAGIC ORDER BY total_rejected DESC
# MAGIC LIMIT 3;
# MAGIC

# COMMAND ----------

joined_df.write.mode("overwrite").parquet("/Volumes/projectsworks/default/data/smith_material_rejection_parquet")
