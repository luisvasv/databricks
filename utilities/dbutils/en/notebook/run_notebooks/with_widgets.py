# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://is2-ssl.mzstatic.com/image/thumb/Purple114/v4/f4/c5/51/f4c551c7-7c20-1103-9738-e761e4a89ade/source/200x200bb.jpg" alt="ConboBox" style="width: 200">
# MAGIC </div>

# COMMAND ----------

log = []
log.append("[STEP 1] showing data.")
log.append("arg received # 1 : " + dbutils.widgets.get("name"))
log.append("arg received # 2 : " + dbutils.widgets.get("email"))
dbutils.notebook.exit("/n".join(log))


# COMMAND ----------

"""
  dbutils.widgets.text(
    name="name",
    defaultValue="",
    label="name")


  dbutils.widgets.text(
    name="email",
    defaultValue="",
    label="email")
""""
