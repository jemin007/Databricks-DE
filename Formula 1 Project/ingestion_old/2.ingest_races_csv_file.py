# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestiion of Races CSV file

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dlcourse/raw

# COMMAND ----------

from pyspark.sql.types import (IntegerType, StringType, DateType, StructField, StructType)

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.json("/mnt/f1dlcourse/raw/races.json")

# COMMAND ----------

display(races_df)
races_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

modified_races_df = races_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))
modified_races_df.show()

# COMMAND ----------

final_races_df = modified_races_df \
.withColumnRenamed("circuitId","circuit_id") \
.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(' '),col("time")),"yyyy-MM-dd HH:mm:ss")) \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_races_df)

# COMMAND ----------

final_races_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/f1dlcourse/processed/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/f1dlcourse/processed/races"))

# COMMAND ----------


