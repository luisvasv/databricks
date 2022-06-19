# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://www.mentromax.com/img/Azure-Data-Factory-Logo.png" alt="ConboBox" style="width: 200">
# MAGIC </div>

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

df = spark.read.json(dbutils.widgets.get("path"))


# COMMAND ----------

df.head()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()
