-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Finding dominant drivers

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------

select driver_name,
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2010 and 2020
group by driver_name
having count(*) >= 50
order by 3 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Finding dominant teams

-- COMMAND ----------

select team_name,
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2010 and 2020
group by team_name
having count(*) >= 100
order by 3 desc

-- COMMAND ----------


