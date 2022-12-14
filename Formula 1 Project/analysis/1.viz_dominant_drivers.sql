-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = "<h1 text-align:center> Report on dominant drivers and teams </h1>"
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_driver as (
select driver_name,
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points,
RANK() OVER(ORDER BY avg(calc_points) desc ) as driver_rank
from f1_presentation.calculated_race_results
group by driver_name
having count(*) >= 50
)

-- COMMAND ----------


select 
race_year,
driver_name,
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_driver where driver_rank <=10)
group by race_year, driver_name
order by 1,5 desc


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Finding dominant teams

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------

create or replace temp view v_team as 
select 
team_name, count(*) as total_race, sum(calc_points) as total_points,  avg(calc_points) as avg_points,
rank() over(order by avg(calc_points) desc ) as rnk
from f1_presentation.calculated_race_results
group by  team_name 
having count(*) >=100


-- COMMAND ----------

select 
race_year, team_name, count(*) as total_race, sum(calc_points) as total_points,  avg(calc_points) as avg_points
from f1_presentation.calculated_race_results
where team_name in (select team_name from v_team where rnk <=5)
group by  race_year, team_name 
order by 1, 5 desc

-- COMMAND ----------


