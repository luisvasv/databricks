-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # CHANGE DATA CAPTTURE (CDC)
-- MAGIC
-- MAGIC Change Data Capture (CDC) is a technique used in Databricks to capture and track changes in data from a data source. This allows users to identify and analyze specific changes in data, rather than having to process all of the data from the data source every time it is updated.
-- MAGIC
-- MAGIC In Databricks, CDC can be implemented using the functionality of Apache Spark Structured Streaming, which is a real-time processing engine that allows users to analyze data in motion. With CDC enabled in Spark Structured Streaming, users can capture changes in source data and process them in real-time to feed analytics applications, dashboards, and other real-time applications.
-- MAGIC
-- MAGIC CDC is particularly useful in environments where data changes frequently and users need to process only the most recent changes. Additionally, CDC also helps reduce latency and improve data processing performance, which is especially important in real-time applications.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## IMPORTING LIBRARIES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as fn

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DEFINE FUNCTIONS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def bronze_to_silver():
-- MAGIC     spark.readStream.table("test.bronze")\
-- MAGIC         .withColumn("processed_time", fn.current_timestamp())\
-- MAGIC         .withColumn("salary", fn.col("hour_value") * fn.col("hours_worked"))\
-- MAGIC         .select("id","name", "salary", "processed_time")\
-- MAGIC         .writeStream.option("checkpointLocation", '/mnt/silverCheckpoint').trigger(once=True).table("test.silver")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #write multiple tables | add transformations
-- MAGIC def write_multi_option(micro_batch, batch_id):
-- MAGIC     appId = 'write_twice'
-- MAGIC     
-- MAGIC     micro_batch.select("id", "name", fn.current_timestamp().alias("processed_time")).write.option("txnVersion", batch_id).option("txnAppId", appId).mode("append").saveAsTable("test.silver_a")
-- MAGIC     
-- MAGIC     micro_batch.select("id", "hour_value", fn.current_timestamp().alias("processed_time")).write.option("txnVersion", batch_id).option("txnAppId", appId).mode("append").saveAsTable("test.silver_b")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CREATING STRUCTURES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SIMPLE

-- COMMAND ----------

CREATE TABLE test.bronze (
  id INT,
  name STRING,
  hour_value DOUBLE,
  hours_worked INT
)
LOCATION '/mnt/bronze/bronze'; 

-- COMMAND ----------

CREATE TABLE test.silver (
  id INT,
  name STRING,
  salary DOUBLE,
  processed_time TIMESTAMP
)
LOCATION '/mnt/silver/silver'; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### MULTIPLE

-- COMMAND ----------

CREATE TABLE test.bronze_2 (
  id INT,
  name STRING,
  hour_value DOUBLE,
  hours_worked INT
)
LOCATION '/mnt/bronze/bronze_2'; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DEMO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SIMPLE DESTINATION

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### INSERTING

-- COMMAND ----------

INSERT INTO test.bronze VALUES 
(1, 'demo-1', 15, 3),
(2, 'demo-2', 13, 43)
;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # CDC
-- MAGIC bronze_to_silver()

-- COMMAND ----------

SELECT * FROM test.silver

-- COMMAND ----------

INSERT INTO test.bronze VALUES (3, 'demo-31', 50, 200);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # CDC
-- MAGIC bronze_to_silver()

-- COMMAND ----------

SELECT * FROM test.silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### UPDATING

-- COMMAND ----------

UPDATE test.bronze
set hours_worked = 253
WHERE id=1;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # nativ CDC doest support directly updating or deleting, we need to modify the tables
-- MAGIC # the concep change to : change data feed (CDF)
-- MAGIC bronze_to_silver()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### MULTIPLE DESTINATION

-- COMMAND ----------

INSERT INTO test.bronze_2 VALUES 
(1, 'demo-1', 15, 3),
(2, 'demo-2', 13, 43)
;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.readStream.table("test.bronze_2")\
-- MAGIC     .writeStream\
-- MAGIC     .foreachBatch(write_multi_option)\
-- MAGIC     .outputMode("update")\
-- MAGIC     .option("checkpointLocation", '/mnt/silverCheckpoint_2')\
-- MAGIC     .trigger(once=True)\
-- MAGIC     .start()

-- COMMAND ----------

select * from test.silver_a

-- COMMAND ----------

select * from test.silver_b

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### MULTIPLE STREAMING
-- MAGIC
-- MAGIC Incremental aggregation can be useful for a number of purposes, including dashboarding and enriching reports with current summary data.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.readStream
-- MAGIC          .table("<table>").actions.(withcolumn, etc)
-- MAGIC          .writeStream # a
-- MAGIC          .option("checkpointLocation", keyValueCheckpoint)
-- MAGIC          .outputMode("complete")
-- MAGIC          .trigger(once=True)
-- MAGIC          .table("key_value"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SET SQL MICROBATCH

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def upsert_cdc(microBatchDF, batchID):
-- MAGIC     microBatchDF.createTempView("bronze_batch") # actions
-- MAGIC     
-- MAGIC     query = "SELECT .."
-- MAGIC     microBatchDF._jdf.sparkSession().sql(query) # set SQL 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### STOP STREAMING

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Stop Streaming Job
-- MAGIC for stream in spark.streams.active:
-- MAGIC   stopped = True
-- MAGIC   stream.stop()
