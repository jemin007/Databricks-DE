# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Ingesting constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read JSON File using spark dataframe reader API
# MAGIC ###### Using DDL format to specify schema

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dlcourse/raw

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_schema) \
.option("header", True) \
.json("/mnt/f1dlcourse/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Dropping and transforming data

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Writing data to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/f1dlcourse/processed/constructors")

# COMMAND ----------

display(spark.read.parquet("/mnt/f1dlcourse/processed/constructors"))

# COMMAND ----------


