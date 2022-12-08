-- Databricks notebook source
create database if not exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database extended demo; 

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables;

-- COMMAND ----------


