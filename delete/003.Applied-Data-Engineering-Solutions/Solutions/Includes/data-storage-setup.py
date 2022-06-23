# Databricks notebook source
# MAGIC %run ./data-source

# COMMAND ----------

# MAGIC %run ./_user $lesson="ds"

# COMMAND ----------

import pyspark.sql.functions as F
import re

# Moved to _user
# dbutils.widgets.text("course", "ds")
# course = dbutils.widgets.get("course")
# username = spark.sql("SELECT current_user()").collect()[0][0]
# userhome = f"dbfs:/user/{username}/{course}"
# database = f"""{course}_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""
# print(f"""
# username: {username}
# userhome: {userhome}
# database: {database}""")

spark.sql(f"SET c.username = {username}")
spark.sql(f"SET c.userhome = {userhome}")
spark.sql(f"SET c.database = {database}")

dbutils.widgets.text("mode", "setup")
mode = dbutils.widgets.get("mode")

if mode == "reset":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

if mode != "cleanup":
    (spark.read
         .load(f"{URI}/bronze")
         .select(F.col("key").cast("string").alias("key"), "value", "topic", "partition", "offset", "timestamp")
         .filter("week_part > '2019-49'")
         .write
         .format("parquet")
         .save(f"{userhome}/raw_parquet"))
    
    spark.read.format("parquet").load(f"{userhome}/raw_parquet").createOrReplaceTempView("raw_data")

if mode == "cleanup":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)

