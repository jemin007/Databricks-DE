# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesting Multiline JSON file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json("/mnt/f1dlcourse/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_pit_stops_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_pit_stops_df)

# COMMAND ----------

final_pit_stops_df.write.mode("overwrite").parquet('/mnt/f1dlcourse/processed/pit_stops')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/mnt/f1dlcourse/processed/pit_stops

# COMMAND ----------


