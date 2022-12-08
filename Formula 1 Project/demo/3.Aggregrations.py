# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

demo_df = spark.read.parquet(f"{presentation_folder_path}/race_results").filter("race_year == 2020")

# COMMAND ----------

demo_df.show()

# COMMAND ----------

from pyspark.sql.functions import count, sum, max, min, countDistinct

# COMMAND ----------

demo_df \
.groupBy("driver_name") \
.agg(sum("points").alias("total_points"), count("race_name").alias("total_races")) \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window Functions

# COMMAND ----------

demo_df = spark.read.parquet(f"{presentation_folder_path}/race_results").filter("race_year in (2019,2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df \
.groupBy("race_year", "driver_name") \
.agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))


# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, dense_rank

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank", dense_rank().over(driverRankSpec)).show(100)

# COMMAND ----------


