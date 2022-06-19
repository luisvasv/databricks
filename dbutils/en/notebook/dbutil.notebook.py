# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # NOTEBOOK
# MAGIC 
# MAGIC Permite realizar acciones con los notebooks, como ejecutarlos entre otras cosas y manejar codigos de ejecucion en los mismos

# COMMAND ----------

# MAGIC %md
# MAGIC ## HELP

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET NOTEBOOK PATH

# COMMAND ----------

# MAGIC %md
# MAGIC ### PYTHON

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCALA

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.notebook.getContext.notebookPath

# COMMAND ----------

# MAGIC %md
# MAGIC ## RUN
# MAGIC `run(path: String, timeoutSeconds: int, arguments: Map): String -> This method runs a notebook and returns its exit value`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ```
# MAGIC [ERROR DETECTED] 18/06/2022
# MAGIC 
# MAGIC The documentation from scala and python is incompatible, by default the information showed correspondo to scala, for python execute `help`
# MAGIC 
# MAGIC 
# MAGIC ```

# COMMAND ----------

help(dbutils.notebook.run)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OK

# COMMAND ----------

# MAGIC %md
# MAGIC #### PYTHON

# COMMAND ----------

main_path:str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
main_path = "/".join(main_path.split("/")[:-1]) + "/run_notebooks/without_error"

print("location : ", main_path)
dbutils.notebook.run(
  path=main_path,
  timeout_seconds=60,
  arguments={}
)

# COMMAND ----------

execution_code = int(dbutils.notebook.run(
  path=main_path,
  timeout_seconds=60,
  arguments={}
))

print(f"the execution code is {execution_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SCALA

# COMMAND ----------

# MAGIC %scala
# MAGIC var main_path = dbutils.notebook.getContext.notebookPath.get
# MAGIC main_path = main_path.split("/").dropRight(1).mkString("/") + "/run_notebooks/without_error"
# MAGIC 
# MAGIC println(s"location : ${main_path}")
# MAGIC dbutils.notebook.run(
# MAGIC   path=main_path,
# MAGIC   timeoutSeconds=60,
# MAGIC   arguments=Map()
# MAGIC )

# COMMAND ----------

# MAGIC %scala
# MAGIC val execution_code = dbutils.notebook.run(
# MAGIC   path=main_path,
# MAGIC   timeoutSeconds=60,
# MAGIC   arguments=Map()
# MAGIC ).toInt
# MAGIC 
# MAGIC println(f"the execution code is ${execution_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ERRORS

# COMMAND ----------

# MAGIC %md
# MAGIC #### PYTHON

# COMMAND ----------

main_path:str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
main_path = "/".join(main_path.split("/")[:-1]) + "/run_notebooks/with_error"

print("location : ", main_path)
dbutils.notebook.run(
  path=main_path,
  timeout_seconds=60,
  arguments={}
)

# COMMAND ----------

execution_code = int(dbutils.notebook.run(
  path=main_path,
  timeout_seconds=60,
  arguments={}
))

print(f"the execution code is {execution_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SCALA

# COMMAND ----------

# MAGIC %scala
# MAGIC var main_path = dbutils.notebook.getContext.notebookPath.get
# MAGIC main_path = main_path.split("/").dropRight(1).mkString("/") + "/run_notebooks/with_error"
# MAGIC 
# MAGIC println(s"location : ${main_path}")
# MAGIC dbutils.notebook.run(
# MAGIC   path=main_path,
# MAGIC   timeoutSeconds=60,
# MAGIC   arguments=Map()
# MAGIC )

# COMMAND ----------

# MAGIC %scala
# MAGIC val execution_code = dbutils.notebook.run(
# MAGIC   path=main_path,
# MAGIC   timeoutSeconds=60,
# MAGIC   arguments=Map()
# MAGIC ).toInt
# MAGIC 
# MAGIC println(f"the execution code is ${execution_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ARGUMENTS

# COMMAND ----------

# MAGIC %md
# MAGIC #### PYTHON

# COMMAND ----------

main_path:str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
main_path = "/".join(main_path.split("/")[:-1]) + "/run_notebooks/with_args"

print("location : ", main_path)
dbutils.notebook.run(
  path=main_path,
  timeout_seconds=60,
  arguments={
    "version" : 1.0,
    "technology": "databricks"
  }
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SCALA

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ```
# MAGIC [ERROR DETECTED] 18/06/2022
# MAGIC 
# MAGIC Tdbutils.notebook.run, param: arguments, it don´t support multi data types like python, arguments must be strings in key and value.
# MAGIC 
# MAGIC 
# MAGIC ```

# COMMAND ----------

# MAGIC %scala
# MAGIC var main_path = dbutils.notebook.getContext.notebookPath.get
# MAGIC main_path = main_path.split("/").dropRight(1).mkString("/") + "/run_notebooks/with_args"
# MAGIC 
# MAGIC println(s"location : ${main_path}")
# MAGIC dbutils.notebook.run(
# MAGIC   path=main_path,
# MAGIC   timeoutSeconds=60,
# MAGIC   arguments=Map(
# MAGIC     "version" -> "1.0",
# MAGIC     "technology" -> "databricks"
# MAGIC   )
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### WIDGETS (%run)

# COMMAND ----------

# MAGIC %run ./run_notebooks/with_widgets $name="luis" $email="demo@org.com"

# COMMAND ----------

# MAGIC %run ./run_notebooks/with_widgets $name="databricks" $email="databricks@org.com"
