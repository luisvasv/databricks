-- Databricks notebook source
-- MAGIC %md
-- MAGIC # VARIABLES
-- MAGIC 
-- MAGIC variables that will be used in `SQL`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## HIVE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SET
-- MAGIC Sets a property, returns the value of an existing property or returns all SQLConf properties with value and meaning.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### SQL SET 

-- COMMAND ----------

SET demo.config.engine='databricks & delta';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### SPARK SQL 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("SET demo.config.user=training")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SPARK CONF 

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC spark.conf.set('demo.config.namespace','test')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### GET

-- COMMAND ----------

SELECT "${demo.config.user}", "${demo.config.namespace}", "${demo.config.engine}"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### RESET
-- MAGIC 
-- MAGIC Resets runtime configurations specific to the current session which were set via the SET command to your default values.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## PYTHON

-- COMMAND ----------

-- MAGIC %run ../../utilities/variables/python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(CONSTANTS.OK)
-- MAGIC print(CONSTANTS.ERROR)
-- MAGIC print(CONSTANTS.ENGINE)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SCALA

-- COMMAND ----------

-- MAGIC %run ../../utilities/variables/scala

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC println(CONSTANTS.OK)
-- MAGIC println(CONSTANTS.ERROR)
-- MAGIC println(CONSTANTS.ENGINE)
