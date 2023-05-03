-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # FUNCTIONS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## ADMINS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### INPUT FILE NAME

-- COMMAND ----------


SELECT *, input_file_name() FROM test.dml;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### INPUT FILE BLOCK LENGTH

-- COMMAND ----------

SELECT *,input_file_block_length() FROM test.dml;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### CURRENT USER
-- MAGIC
-- MAGIC return the current user name

-- COMMAND ----------

SELECT current_user()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### IS MEMBER
-- MAGIC determine if the current user is a member of a specific Databricks group

-- COMMAND ----------

SELECT is_member("pepito"), is_member("admins")

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## DATES

-- COMMAND ----------



-- COMMAND ----------

current_timestamp()
