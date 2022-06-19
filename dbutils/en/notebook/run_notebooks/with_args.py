# Databricks notebook source
log = []
log.append("[STEP 1] creating widgets ..")
dbutils.widgets.text(
  name="version",
  defaultValue="version_demoDefault",
  label="version_demoEmptyLabel")


dbutils.widgets.text(
  name="technology",
  defaultValue="technologyoDefault",
  label="technologyEmptyLabel")

# COMMAND ----------

log.append("[STEP 2] showing data.")
log.append("arg received # 1 : " + dbutils.widgets.get("version"))
log.append("arg received # 2 : " + dbutils.widgets.get("technology"))
dbutils.notebook.exit("/n".join(log))
