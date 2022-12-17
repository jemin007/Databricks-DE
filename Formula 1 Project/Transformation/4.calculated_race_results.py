# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %sql --drop table f1_presentation.calculated_race_results

# COMMAND ----------

spark.sql("""
        create table if not exists f1_presentation.calculated_race_results
        (
        race_year int,
        team_name string,
        driver_id int,
        driver_name string,
        race_id int,
        position int,
        points int,
        calculated_points int,
            created_date timestamp,
            updated_date timestamp
        )
        USING DELTA
        """)

# COMMAND ----------

spark.sql(f"""
            create or replace temp view race_results_updated
            as
            select ra.race_year,
                    c.name as team_name,
                    d.driver_id,
                    d.name as driver_name,
                    ra.race_id,
                    r.position,
                    r.points,
                    11 - r.position as calculated_points
              from f1_processed.results r join f1_processed.drivers d on (d.driver_id = r.driver_id)
              join f1_processed.constructors c on (c.constructor_id = r.constructor_id)
              join f1_processed.races ra on (ra.race_id = r.race_id)
              where r.position <=10 AND r.file_date = '{v_file_date}'
  """)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_presentation.calculated_race_results tgt
# MAGIC USING race_results_updated upd
# MAGIC ON tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.position = upd.position,
# MAGIC     tgt.points = upd.points,
# MAGIC     tgt.calculated_points = upd.calculated_points,
# MAGIC     tgt.updated_date = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     race_year,team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     race_year,team_name,  driver_id, driver_name, race_id, position, points, calculated_points, current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.calculated_race_results

# COMMAND ----------


