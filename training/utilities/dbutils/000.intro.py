# Databricks notebook source
# MAGIC %md
# MAGIC Databricks provide a lot of functionalities that you can integrate inside your notebooks directly, the root to call this utility is dbutils `dbutil`. In this section, we understand and analyze every module how it works, and when it is appropriate to use. 
# MAGIC 
# MAGIC To use it, you must have a cluster running. For more information about module provides, execute: `dbutils.help()`

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC es importante aclarar que dbsutils puede ser accedida por python y por scala, la idea es verificar bien este paquete, ya que se he observado diferentes variaciones en su funcionamiento dependiendo del lenguaje.
