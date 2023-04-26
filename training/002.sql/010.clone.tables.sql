-- Databricks notebook source
-- MAGIC %md
-- MAGIC # CLONE TABLES
-- MAGIC 
-- MAGIC Cloned tables in Databricks are an exact copy of an existing table in a Databricks cluster. They are used to perform analysis and operations on the copy of the table, without affecting the original, which allows tests and experiments to be carried out without the risk of damaging the original data.
-- MAGIC 
-- MAGIC Additionally, cloned tables can be used to improve query performance as they can be distributed across different cluster nodes and processed in parallel. They can also be used to share data and collaborate with other users, as each user can have their own copy of the table to work on.
-- MAGIC 
-- MAGIC exists two clone types:
-- MAGIC 
-- MAGIC * **DEEP CLONE**: (default), Databricks will make a complete and independent copy of the source table.
-- MAGIC * **SHALLOW CLONE**: make a copy of the source table definition, but refer to the source table files
-- MAGIC 
-- MAGIC 
-- MAGIC ```
-- MAGIC CREATE TABLE [IF NOT EXISTS] table_name
-- MAGIC    [SHALLOW | DEEP] CLONE source_table_name [TBLPROPERTIES clause] [LOCATION path]
-- MAGIC 
-- MAGIC [CREATE OR] REPLACE TABLE table_name
-- MAGIC    [SHALLOW | DEEP] CLONE source_table_name [TBLPROPERTIES clause] [LOCATION path]
-- MAGIC ```
-- MAGIC 
-- MAGIC [reference](https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/delta-clone)
-- MAGIC 
-- MAGIC To unserstad this amazing concept, let's create a table:

-- COMMAND ----------

-- MAGIC %run ../../complements/variables/set_using_python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC url: str = training.dataset.social_ads
-- MAGIC df = spark.createDataFrame(pd.read_csv(url))
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta").partitionBy('age').saveAsTable("test.original", path="/mnt/bronze/original")

-- COMMAND ----------

DESCRIBE EXTENDED test.original;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## SHALLOW CLONE
-- MAGIC 
-- MAGIC make a copy of the source table definition, but refer to the source table files

-- COMMAND ----------

-- create shallow clone
CREATE TABLE IF NOT EXISTS test.clone_shallow 
SHALLOW CLONE test.original 
LOCATION '/mnt/bronze/clone_shallow';

-- COMMAND ----------

-- validate original table
SELECT COUNT(1) amount_data FROM test.original  ;

-- COMMAND ----------

SHOW COLUMNS IN test.original

-- COMMAND ----------

-- validate cloned table
SELECT COUNT(1) amount_data FROM test.clone_shallow ;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #if you check the table location, it does not have files 
-- MAGIC dbutils.fs.ls('/mnt/bronze/clone_shallow')

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### ALTER TABLE

-- COMMAND ----------

ALTER TABLE test.clone_shallow ADD COLUMN estimated_salary_v2 STRING;

-- COMMAND ----------

-- checking original
SHOW COLUMNS IN test.original

-- COMMAND ----------

-- checking original
SHOW COLUMNS IN test.clone_shallow

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### INSERT DATA
-- MAGIC 
-- MAGIC Note: same behaviour for dml actions

-- COMMAND ----------

INSERT INTO test.clone_shallow VALUES (15624510,'Male',19,19000,0,'00')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # we have the new record in the shallow table | the new events are added on the shallow table instead of original source
-- MAGIC dbutils.fs.ls('/mnt/bronze/clone_shallow')

-- COMMAND ----------

-- validate original table (wasn't affected)
SELECT COUNT(1) amount_data FROM test.original  ;

-- COMMAND ----------

-- validate cloned table
SELECT COUNT(1) amount_data FROM test.clone_shallow ;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## DEEP CLONE
-- MAGIC 
-- MAGIC (default), Databricks will make a complete and independent copy of the source table.
-- MAGIC 
-- MAGIC for being a independent table, you can apply a normal table validations
-- MAGIC 
-- MAGIC Note: deep is optional

-- COMMAND ----------

-- create shallow clone
CREATE TABLE IF NOT EXISTS test.clone_deep 
DEEP CLONE test.original 
LOCATION '/mnt/bronze/clone_deep';

-- COMMAND ----------

-- validate cloned table
SELECT COUNT(1) amount_data FROM test.clone_deep ;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #if you check the table location, it will have the all files
-- MAGIC dbutils.fs.ls('/mnt/bronze/clone_deep')

-- COMMAND ----------

describe EXTENDED test.clone_deep
