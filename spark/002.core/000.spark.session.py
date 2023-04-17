# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # SPARK SESSION
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://gankrin.org/wp-content/uploads/2019/09/Capture-300x241.jpg" alt="ConboBox" style="width: 200">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ACCESS

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## AVAILABLE METHODS

# COMMAND ----------

class AutoIncrement:
    _value = 0
    def __new__(cls):
        cls._value += 1
        return cls._value

# COMMAND ----------

print("main methods")
print("")
print("\n".join([ "{} --> {} == {}".format(AutoIncrement(),element, eval(f"type(spark.{element})")) for element in dir(spark) if "_" not in element]))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SIMPLE ACCESS

# COMMAND ----------

spark.version

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## MODIFY 
