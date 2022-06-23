# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### TRABAJANDO CON FSUTILS.MOUNT
# MAGIC 
# MAGIC Este servicio sirve para montar datos
# MAGIC 
# MAGIC 1.    'dbutils.fs'
# MAGIC 2.   '%fs'
# MAGIC 3.   '%sh'

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("ls")

# COMMAND ----------

# por linea de comandos
dbutils.fs.ls("/")

# COMMAND ----------

# crear carpeta forma 1
dbutils.fs.mkdirs("/demo-databricks")

# COMMAND ----------

# crear carpeta forma 1
dbutils.fs.mkdirs("/demo-databricks/a")
dbutils.fs.mkdirs("/demo-databricks/b")
dbutils.fs.mkdirs("/demo-databricks/c")
dbutils.fs.mkdirs("/demo-databricks/f/g/h")

# COMMAND ----------

# crear archivos
dbutils.fs.put("/demo-databricks/sayhello.txt", "hello databricks",overwrite=True) # si existe lo regenere
dbutils.fs.put("/demo-databricks/sayhello1.txt", "hello databricks")
dbutils.fs.put("/demo-databricks/sayhello2.txt", "hello databricks")
dbutils.fs.put("/demo-databricks/sayhello3.txt", "hello databricks")


# COMMAND ----------

# copiando archivos simple
dbutils.fs.cp("/demo-databricks/sayhello.txt", "/demo-databricks/sayhello_cp.txt")

# COMMAND ----------

# copiando archivos con recursion
dbutils.fs.cp("/demo-databricks/f", "/demo-databricks/d",recurse=True)

# COMMAND ----------

#mirando segmento de archivo, 
dbutils.fs.head("/demo-databricks/sayhello.txt")

# COMMAND ----------

#truncando los bytes
dbutils.fs.head("/demo-databricks/sayhello.txt")[:3]

# COMMAND ----------

# MAGIC %scala
# MAGIC // mirando segmento de archivo, indicandole los bytes
# MAGIC // mirando segmento de archivo, 
# MAGIC dbutils.fs.head("/demo-databricks/sayhello.txt", maxBytes=3)

# COMMAND ----------

# moviendo archivos
dbutils.fs.mv("/demo-databricks/sayhello.txt", "/demo-databricks/a/")

# COMMAND ----------

# moviendo carpetas
dbutils.fs.cp("/demo-databricks/d","/demo-databricks/x",recurse=True)
dbutils.fs.mv("/demo-databricks/x", "/demo-databricks/c/", recurse=True)

# COMMAND ----------

# eliminando archivos
dbutils.fs.rm("/demo-databricks/sayhello3.txt")

# COMMAND ----------

# eliminando carpets
dbutils.fs.cp("/demo-databricks/d","/demo-databricks/x",recurse=True)
display(dbutils.fs.ls("/demo-databricks/"))
dbutils.fs.rm("/demo-databricks/x", recurse=True)
display(dbutils.fs.ls("/demo-databricks/"))

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC TRABAJANDO CON %FS
# MAGIC 
# MAGIC Es simular a las utilerias de arriba, solo que es por CLI pero aplican los mismpos parametros, solo que se remplazan por --, ejemplo :
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC dbutils.fs.cp("/demo-databricks/f", "/demo-databricks/d",recurse=True)  === %fs cp dbfs:/demo-databricks/f/ dbfs:/demo-databricks/ooo/ --recurse=true

# COMMAND ----------

# MAGIC %fs help

# COMMAND ----------

# MAGIC %fs ls dbfs:/demo-databricks/

# COMMAND ----------

# MAGIC %fs cp dbfs:/demo-databricks/f/ dbfs:/demo-databricks/ooo/ --recurse=true

# COMMAND ----------

# MAGIC %fs head dbfs:/demo-databricks/sayhello1.txt

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC TRABAJANDO CON %SH
# MAGIC 
# MAGIC se usa para acceder no al data file system

# COMMAND ----------

# MAGIC %sh cat /etc/*-release

# COMMAND ----------

# MAGIC %sh cal

# COMMAND ----------


