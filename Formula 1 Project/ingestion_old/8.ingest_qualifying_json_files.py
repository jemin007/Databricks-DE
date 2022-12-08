# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesting Multiple Multiline JSON files

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json("/mnt/f1dlcourse/raw/qualifying")

# COMMAND ----------

display(qualifying_df)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_qualifying_df = qualifying_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("qualifyingId", "qualifying_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_qualifying_df)

# COMMAND ----------

final_qualifying_df.write.mode("overwrite").parquet('/mnt/f1dlcourse/processed/qualifying')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dlcourse/processed/qualifying

# COMMAND ----------


