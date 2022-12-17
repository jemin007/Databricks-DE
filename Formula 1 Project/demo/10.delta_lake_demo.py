# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location '/mnt/f1dlcourse/demo'

# COMMAND ----------

# MAGIC %python
# MAGIC results_df = spark.read \
# MAGIC .option("inferSchema", True) \
# MAGIC .json('/mnt/f1dlcourse/raw/2021-03-28/results.json')

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/f1dlcourse/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.results_external
# MAGIC using DELTA
# MAGIC location "/mnt/f1dlcourse/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

results_ext_df = spark.read.format("delta").load("/mnt/f1dlcourse/demo/results_external")

# COMMAND ----------

display(results_ext_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy('constructorId').saveAsTable('f1_demo.results_partition')

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partition

# COMMAND ----------

# MAGIC %md
# MAGIC Updates and deletes

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

#method 1 to update
%sql
update f1_demo.results_managed
set points = 11-position
where position <=10

# COMMAND ----------

#update using python predicate
#method 2
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/f1dlcourse/demo/results_managed")
deltaTable.update("position <= 10", {"points": "21 - position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.results_managed
# MAGIC where position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.results_managed
# MAGIC where points = 0

# COMMAND ----------

# MAGIC %md
# MAGIC #####Upsert using merge
# MAGIC ####### Inserts data and also merges data with new day datas
# MAGIC ####### Upsert = Update + Insert

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/f1dlcourse/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------


drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/f1dlcourse/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------


drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/f1dlcourse/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 15 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_merge (
# MAGIC driverId int,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Day 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,dob,forename,surname,createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,dob,forename,surname,current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,dob,forename,surname,createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,dob,forename,surname,current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using python

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTablePeople = DeltaTable.forPath(spark, '/mnt/f1dlcourse/demo/drivers_merge')


deltaTablePeople.alias('tgt') \
  .merge(
    drivers_day3_df.alias('upd'),
    'tgt.driverId =upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "updatedDate": "current_timestamp"
      
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
       "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "createdDate": "current_timestamp"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------


