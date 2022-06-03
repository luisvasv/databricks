# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### DBSUTILS.NOTEBOOK
# MAGIC 
# MAGIC Permite realizar acciones con los notebooks, como ejecutarlos entre otras cosas y manejar codigos de ejecucion en los mismos

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %scala
# MAGIC //conociendo la locacion de in notebook en scala 
# MAGIC dbutils.notebook.getContext.notebookPath

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %python
# MAGIC # luego llamamos directamente el texto almacenado
# MAGIC dbutils.widgets.get("notebook")

# COMMAND ----------

# manejando errores en el notebook
# dbutils.notebook.exit(code)

#cuando el proceso del notebook finalice 


# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.notebook.run("/workspace_demo/run_notebooks/with_error", 60, Map("x" -> "3.1416"))
