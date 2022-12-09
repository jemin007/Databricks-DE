-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Managed table examples using SQL and Python
-- MAGIC * Drops data and metadata
-- MAGIC 
-- MAGIC #### Views on tables
-- MAGIC * Temp, Global and Permanent Views

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Creating Managed table using Python
-- MAGIC race_results_df.write.mode("overwrite").format("parquet").saveAsTable("demo.race_results_python")
-- MAGIC #DBName.NewTableName

-- COMMAND ----------

use demo;
show tables

-- COMMAND ----------

describe extended race_results_python

-- COMMAND ----------

select * from demo.race_results_python where race_year = 2019

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(race_results_df.filter("race_year == 2019"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ###Creating table using SQL method

-- COMMAND ----------

use demo;
CREATE TABLE race_result_sql
AS
SELECT * FROM demo.race_results_python
  where race_year = 2019

-- COMMAND ----------

show tables

-- COMMAND ----------

describe extended race_result_sql

-- COMMAND ----------

show tables

-- COMMAND ----------

select current_database()

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Views

-- COMMAND ----------

--Temp_View
CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT * FROM demo.race_results_python WHERE race_year == 2020;


-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Global View

-- COMMAND ----------

--Global_View
CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * FROM demo.race_results_python WHERE race_year == 2019;

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Permanent View

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT * FROM demo.race_results_python WHERE race_year == 2020;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------


