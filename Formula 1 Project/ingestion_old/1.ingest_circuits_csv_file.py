# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Read CSV File using Spark DataFrame Reader 

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/f1dlcourse/raw

# COMMAND ----------

# \ breaks line
# header detects header info automatically
# inferSchema detects data type automatically to string and int

# Is inefficient as inferSchema describes datatype automatically

"""""
circuits_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.csv("dbfs:/mnt/f1dlcourse/raw/circuits.csv")
"""""

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_value_score", " ")
v_value_score = dbutils.widgets.get("p_value_score")

# COMMAND ----------

v_value_score

# COMMAND ----------

from pyspark.sql.types import(StructType, StructField, IntegerType, StringType, DoubleType)

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
    
])

# COMMAND ----------

circuits_df = spark.read \
.option("header",True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

#circuits_df.show()
display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only required columns

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

circuits_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_value_score))

# COMMAND ----------

circuits_renamed_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Ingesting a new column using child notebook

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

circuits_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to data lake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls
# MAGIC /mnt/f1dlcourse/processed/circuits

# COMMAND ----------

df = spark.read.parquet("/mnt/f1dlcourse/processed/circuits")
display(df)

# COMMAND ----------


