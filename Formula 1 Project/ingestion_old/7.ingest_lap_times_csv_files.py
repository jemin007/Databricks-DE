# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesting Multiple CSV files

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("/mnt/f1dlcourse/raw/lap_times")

# COMMAND ----------

display(lap_times_df)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_lap_times_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_lap_times_df)

# COMMAND ----------

final_lap_times_df.write.mode("overwrite").parquet('/mnt/f1dlcourse/processed/lap_times')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dlcourse/processed/lap_times

# COMMAND ----------


