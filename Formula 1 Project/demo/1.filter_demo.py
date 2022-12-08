# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Filtering data

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

transformed_df = races_df.filter("race_year IN (2019,2018) and round <= 8")

# COMMAND ----------

display(transformed_df)

# COMMAND ----------


