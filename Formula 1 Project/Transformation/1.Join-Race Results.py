# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("race_timestamp", "race_date") \
.withColumnRenamed("name", "race_name") 

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
.withColumnRenamed("time", "race_time")

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select("race_year", "race_name", "race_date","race_id","circuit_location")

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

race_results_df = results_df.join(races_circuits_df, results_df.race_id == races_circuits_df.race_id,"inner") \
                     .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
                     .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position") \
.withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name =='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable('f1_presentation.race_results')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dlcourse/presentation/race_results

# COMMAND ----------


