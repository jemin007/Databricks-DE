# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce Constructor Standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

const_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(const_df)

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, rank, when, desc

# COMMAND ----------

const_grp_df = const_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(const_grp_df.filter("race_year == 2020"))

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

const_win = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = const_grp_df.withColumn("rank", rank().over(const_win))
display(final_df.filter("race_year == 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable('f1_presentation.constructor_standings')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dlcourse/presentation/

# COMMAND ----------


