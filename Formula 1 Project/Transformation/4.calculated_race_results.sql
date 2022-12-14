-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

drop table f1_presentation.calculated_race_results;
create table f1_presentation.calculated_race_results
using parquet
as
select ra.race_year,
        c.name as team_name,
        d.name as driver_name,
        r.position,
        r.points,
        11 - r.position as calc_points
  from f1_processed.results r join f1_processed.drivers d on (d.driver_id = r.driver_id)
  join f1_processed.constructors c on (c.constructor_id = r.constructor_id)
  join f1_processed.races ra on (ra.race_id = r.race_id)
  where r.position <=10

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------


