-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### DROP ALL DATABASES

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/f1dlcourse/processed"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/f1dlcourse/presentation"

-- COMMAND ----------


