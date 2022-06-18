# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC para esta sección se deben  tener los secretos y scopes creados, por favor mirar la sección
# MAGIC main repo > cli > secrets

# COMMAND ----------

# MAGIC %md 
# MAGIC # SECRETS USING DBUTILS
# MAGIC 
# MAGIC -----
# MAGIC 
# MAGIC TO DO

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## PYTHON

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### LIST SCOPES
# MAGIC 
# MAGIC si todo esta bien, podras ver el scoop creado en la sección cli >> secrets, el cal tiene el valor databricks-cli

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIST SECRETS

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.secrets.list(scope="databricks-cli")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.secrets.list("databricks-cli")

# COMMAND ----------

# MAGIC %md
# MAGIC ### GET SECRET

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.secrets.get(scope="databricks-cli", key="url_databricks")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.secrets.get("databricks-cli", "url_databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ### GET SECRET FROM BYTES
