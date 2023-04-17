# Databricks notebook source
# MAGIC %md
# MAGIC <img 
# MAGIC      src = https://cdn.cookielaw.org/logos/29b588c5-ce77-40e2-8f89-41c4fa03c155/bc546ffe-d1b7-43af-9c0b-9fcf4b9f6e58/1e538bec-8640-4ae9-a0ca-44240b0c1a20/databricks-logo.png 
# MAGIC      width="700" height="700">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Data Lakehouse
# MAGIC A data lakehouse unifies the best of data warehouses and data lakes in one simple platform to handle all your data, analytics and AI use cases. It’s built on an open and reliable data foundation that efficiently handles all data types and applies one common security and governance approach across all your data and cloud platforms.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta Lake
# MAGIC 
# MAGIC 
# MAGIC Delta Lake is an open-source storage layer designed to run on top of an existing data lake and improve its reliability, security, and performance. It supports ACID transactions, scalable metadata, unified streaming, and batch data processing.
# MAGIC 
# MAGIC <img src = https://www.bbva.com/wp-content/uploads/2020/03/image_0-1920x857.png width="1000" height="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apache Spark
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://spark.apache.org/docs/latest/img/spark-logo-hd.png" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC Apache Spark is a “Sophisticated Distributed Computation Framework” for executing code in Parallel across many different machines.
# MAGIC 
# MAGIC Apache Spark is a unified analytics engine for large-scale data processing. It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs. It also supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing, pandas API on Spark for pandas workloads, MLlib for machine learning, GraphX for graph processing, and Structured Streaming for incremental computation and stream processing.
# MAGIC 
# MAGIC **SPARK API**
# MAGIC 
# MAGIC 
# MAGIC <img src = https://d1.awsstatic.com/Data%20Lake/what-is-apache-spark.b3a3099296936df595d9a7d3610f1a77ff0749df.PNG width="500" height="500"/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC SPARK SQL execute all queries on the same engine
# MAGIC 
# MAGIC 
# MAGIC <img src = "https://github.com/luisvasv/databricks/blob/published/images/spark/000.same.engine.png?raw=true" width="500" height="500">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multiple Languages simultaneously

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ByteType

schema = StructType([
    StructField("language",StringType(), False),
    StructField("type",StringType(), True),
    StructField("supported",StringType()),
    StructField("logic_type",ByteType(), False)
])

data = [
    ("java", "compiled", "y", 1), 
    ("python", "interpreted", "y", 1), 
    ("Scala", "compiled", "y", 1),
    ("r", "interpreted", "y", 1), 
    ("powerbuilder", "unknow", "n", 0), 
    ("c++", "compiled", "n", 0)
]

rdd = spark.sparkContext.parallelize(data)
df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()

# COMMAND ----------

df.write.partitionBy("type").saveAsTable("test.demo")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Scala

# COMMAND ----------

# MAGIC %scala
# MAGIC val scala_df = spark.sql("select * from test.demo LIMIT 1")
# MAGIC scala_df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table test.demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizing Files

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, input_file_name(),input_file_block_length() FROM test.demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO test.demo VALUES ("JS", "compiled", "y", 1);
# MAGIC INSERT INTO test.demo VALUES ("Ruby", "compiled", "y", 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, input_file_name(),input_file_block_length() FROM test.demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Travel
# MAGIC 
# MAGIC We can use a query an old snapshot of a table using time travel. Time travel is a specialized read on our dataset which allows us to read previous versions of our data. There are several ways to go about this. 
# MAGIC 
# MAGIC The `@` symbol can be used with a version number, aliased to `v#`, like the syntax below.
# MAGIC 
# MAGIC The `@` symbol can also be used with a version number or a datestamp in the format: `yyyyMMddHHmmssSSS`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test.demo@v1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test.demo VERSION AS OF 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking History
# MAGIC 
# MAGIC Returns provenance information, including the operation, user, and so on, for each write to a table. Table history is retained for 30 days.
# MAGIC 
# MAGIC **COLUMNS**
# MAGIC 
# MAGIC | Column              | Type      | Description                                                                |
# MAGIC |---------------------|-----------|----------------------------------------------------------------------------|
# MAGIC | version             | long      | Table version generated by the operation.                                  |
# MAGIC | timestamp           | timestamp | When this version was committed.                                           |
# MAGIC | userId              | string    | ID of the user that ran the operation.                                     |
# MAGIC | userName            | string    | Name of the user that ran the operation.                                   |
# MAGIC | operation           | string    | Name of the operation.                                                     |
# MAGIC | operationParameters | map       | Parameters of the operation (for example, predicates.)                     |
# MAGIC | job                 | struct    | Details of the job that ran the operation.                                 |
# MAGIC | notebook            | struct    | Details of notebook from which the operation was run.                      |
# MAGIC | clusterId           | string    | ID of the cluster on which the operation ran.                              |
# MAGIC | readVersion         | long      | Version of the table that was read to perform the write operation.         |
# MAGIC | isolationLevel      | string    | Isolation level used for this operation.                                   |
# MAGIC | isBlindAppend       | boolean   | Whether this operation appended data.                                      |
# MAGIC | operationMetrics    | map       | Metrics of the operation (for example, number of rows and files modified.) |
# MAGIC | userMetadata        | string    | User-defined commit metadata if it was specified                           |

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY test.demo;

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/mnt/bronze/demo/_delta_log/'))

# COMMAND ----------

with open('/dbfs/mnt/bronze/demo/_delta_log/00000000000000000000.json', 'r') as file:
    for line in file:
        print(line)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization (Parquet Vs Delta Lake)
# MAGIC 
# MAGIC The Parquet format organizes data files into directories. Applications interact with these directories as a unified collection of data. This hierarchical design is optimized for distributed read and write operations.
# MAGIC 
# MAGIC <img src = https://i.postimg.cc/qM7C7b9N/files.png width="350" height="450">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Small Files
# MAGIC sometimes we end up with a lot of small files. This is often the result of many incremental updates and/or bad partition choices.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <img src = https://i.postimg.cc/mgpxsD7c/block-size.png width="700" height="700">
# MAGIC 
# MAGIC <img src = https://i.postimg.cc/8CXz9tnv/small.png width="500" height="500">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Big Files (Data Bias)
# MAGIC 
# MAGIC Data bias refers to an unbalanced distribution of data.
# MAGIC 
# MAGIC <img src = https://i.postimg.cc/QNKcgnfv/big-files.png width="500" height="500">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Corrupt Data
# MAGIC 
# MAGIC In Big Data, first read, then validate
# MAGIC 
# MAGIC <img src = https://i.postimg.cc/J087kn6r/corrupt.png width="500" height="500">

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE test.demo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, input_file_name(),input_file_block_length() FROM test.demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY test.demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restore

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE test.demo TO VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, input_file_name(),input_file_block_length() FROM test.demo;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Basic Graph

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT type, count(1) as counter
# MAGIC FROM test.demo
# MAGIC Group by type

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Files in Databricks Repos
# MAGIC 
# MAGIC This notebook shows how you can work with arbitrary files in Databricks Repos. Some common use cases are:
# MAGIC 
# MAGIC * Custom Python and R modules. When you include these modules in a repo, notebooks in the repo can access their functions using an import statement.
# MAGIC * Define an environment in a requirements.txt file in the repo. Then just run `pip install -r requirements.txt` from a notebook to install the packages and create the environment for the notebook.
# MAGIC * Include small data files in a repo. This can be useful for development and unit testing. The maximum size for a data file in a repo is `100 MB`. Databricks Repos provides an editor for small files `(< 10 MB)`.
# MAGIC 
# MAGIC <img src = https://www.databricks.com/wp-content/uploads/2021/10/repos-ga-blog-img-1.png width="500" height="500">

# COMMAND ----------

# MAGIC %pip install tabulate

# COMMAND ----------

from libraries.demos.exceptions import RealDemoException

# COMMAND ----------

try:
 1/0
except Exception as ex:
    we = RealDemoException(str(ex), "Intro Data Lakehouse", type(ex).__name__ )
    we(ex.__traceback__)  # for system exceptions this action is required
    print("formatted exception --->>:")
    print(we)             # output # 1
    print("json exception --->>:")
    print(we._to_json())  # output # 2
    print("")
    print("table exception --->>:")
    print(we._to_table()) # output # 3
    print("")
    print("dict exception --->>:")
    print(we._dict())     # output # 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Events About Databricks
# MAGIC 
# MAGIC ---
# MAGIC <img src = https://www.chauvetprofessional.com/wp-content/uploads/2020/04/tech_talk.jpg width="500" height="500">
# MAGIC 
# MAGIC 
# MAGIC ---
# MAGIC * **Delta Live Tables**
# MAGIC * **Auto Loader**
# MAGIC * **Clone tables**
# MAGIC * **Workflows**
# MAGIC * **Merge Operatios (Upserts, etc)**
# MAGIC * **Change Data Capture (CDC)**
# MAGIC * **Bloom Filter Indexes**
# MAGIC * **ZOrder**
# MAGIC * **Multi-Hop**
# MAGIC ---
# MAGIC 
# MAGIC <img src = https://es.thankyou.co/uploads/Thankyou_Standard_Logo_RGB.png width="500" height="500">
