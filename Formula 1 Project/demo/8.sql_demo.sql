-- Databricks notebook source
show databases;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use f1_processed;
show tables;

-- COMMAND ----------

select * from drivers;

-- COMMAND ----------


desc drivers

-- COMMAND ----------

use f1_presentation

-- COMMAND ----------

desc driver_standings

-- COMMAND ----------

create or replace temp view v_driver_standings_2018
as
select race_year, driver_name, team, total_points, wins, rank
from driver_standings
where race_year = 2018;

-- COMMAND ----------

create or replace temp view v_driver_standings_2020
as
select race_year, driver_name, team, total_points, wins, rank
from driver_standings
where race_year = 2020;

-- COMMAND ----------

select * from v_driver_standings_2020

-- COMMAND ----------

select *
from v_driver_standings_2018 d_18 inner join v_driver_standings_2020 d_20
on d_18.driver_name = d_20.driver_name

-- COMMAND ----------

select *
from v_driver_standings_2018 d_18 semi join v_driver_standings_2020 d_20
on d_18.driver_name = d_20.driver_name

-- COMMAND ----------

select *
from v_driver_standings_2018 d_18 anti join v_driver_standings_2020 d_20
on d_18.driver_name = d_20.driver_name
--raced in 2018 but not in 2020

-- COMMAND ----------


