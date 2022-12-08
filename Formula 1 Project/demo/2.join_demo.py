# Databricks notebook source
# MAGIC %md 
# MAGIC #### Joining data (Inner Join)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
.withColumnRenamed("name", "race_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("name", "circuits_name")

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner" ) \
.select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round )

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

races_circuits_df.select("circuits_name").show(truncate = False)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
.select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round )

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Semi Join
# MAGIC ###### Similar to Inner join but only pulls columns from left table

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anti Join 
# MAGIC * Similar to Semi join but exluces match records from left table

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

races_circuits_df.show()

# COMMAND ----------


