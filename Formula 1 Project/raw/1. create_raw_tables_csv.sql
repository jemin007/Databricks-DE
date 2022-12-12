-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### For CSV Files

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

show databases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits table

-- COMMAND ----------

USE f1_raw;
DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
circuitId int,
circuitRef String,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/f1dlcourse/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create races tables

-- COMMAND ----------

USE f1_raw;
DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
race_id int,
year int,
round int,
cicruitId int,
name STRING,
date DATE,
time STRING,
url STRING
)
USING CSV 
OPTIONS (path "/mnt/f1dlcourse/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### JSON Files

-- COMMAND ----------

USE f1_raw;
DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING)
USING JSON
OPTIONS (path "/mnt/f1dlcourse/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------

USE f1_raw;
DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS (path "/mnt/f1dlcourse/raw/drivers.json")

-- COMMAND ----------

USE f1_raw;
DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS (path "/mnt/f1dlcourse/raw/results.json")

-- COMMAND ----------

USE f1_raw;
DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS (path "/mnt/f1dlcourse/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating multi files CSV

-- COMMAND ----------

USE f1_raw;
DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/f1dlcourse/raw/lap_times")

-- COMMAND ----------

select count(*) from f1_raw.lap_times

-- COMMAND ----------

USE f1_raw;
DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "/mnt/f1dlcourse/raw/qualifying", multiLine true)

-- COMMAND ----------

DESC EXTENDED f1_raw.drivers;

-- COMMAND ----------


