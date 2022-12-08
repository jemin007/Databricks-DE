-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Createa external tables using Python and SQL

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Creating External table using Python and specyfying path
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py" ).saveAsTable("demo.race_results_ext_py")
-- MAGIC #DBName.NewTableName

-- COMMAND ----------

desc EXTENDED demo.race_results_ext_py

-- COMMAND ----------


 
