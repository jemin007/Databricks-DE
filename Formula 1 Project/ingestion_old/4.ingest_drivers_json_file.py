# Databricks notebook source
from pyspark.sql.types import (IntegerType, StringType, DateType, StructField, StructType)

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True),
])

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), True),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json("/mnt/f1dlcourse/raw/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp

# COMMAND ----------

drop_df = drivers_df.drop('url')

# COMMAND ----------

drivers_final_df = drop_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("name", concat("name.forename", lit (' '), "name.surname")) \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/f1dlcourse/processed/drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dlcourse/processed/drivers

# COMMAND ----------


