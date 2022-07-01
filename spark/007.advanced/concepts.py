# Databricks notebook source
# MAGIC %md
# MAGIC ## CLONE
# MAGIC It's important to understand how cloned tables behave with file retention actions.
# MAGIC 
# MAGIC Run the cell below to `VACUUM` your source production table (removing all files not referenced in the most recent version).

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
spark.sql("VACUUM cloned_table RETAIN 0 HOURS")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)
