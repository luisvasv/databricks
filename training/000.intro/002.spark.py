# Databricks notebook source
# MAGIC %md
# MAGIC # APACHE SPARK
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
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://d1.awsstatic.com/Data%20Lake/what-is-apache-spark.b3a3099296936df595d9a7d3610f1a77ff0749df.PNG
# MAGIC " alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC SPARK SQL execute all queries on the same engine
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://github.com/luisvasv/databricks/blob/published/images/spark/000.same.engine.png?raw=true" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC SPARK Optimization
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://github.com/luisvasv/databricks/blob/published/images/spark/001.optimization.png?raw=true" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC https://www.linkedin.com/pulse/apache-spark-rdd-dataframe-dataset-which-one-should-i-reyes-s%C3%A1nchez/

# COMMAND ----------

# MAGIC %md
# MAGIC ## SPARK COMPONENTS
# MAGIC 
# MAGIC the some aspects has been taken from [Oindrila Chakraborty](https://faun.pub/introduction-to-apache-spark-in-databricks-b9bdae56a9a5)
# MAGIC 
# MAGIC Apache Spark is a `Sophisticated Distributed Computation Framework` for executing code in Parallel across many different machines. While the Abstraction and Interfaces are simple, managing `Clusters of Computers` and ensuring`Production-Level Stability` is not. Databricks makes Big Data simple by providing Apache Spark as a `Hosted Solution`, where much of the Spark setup will be managed by Databricks for the Users.
# MAGIC 
# MAGIC Apache Spark uses `Clusters of Computers` to process Big Data by breaking a large Task into smaller ones and distributing the work among several Machines. Following are the Components that Apache Spark uses to co-ordinate work across a “Clusters of Computers”.

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCHEDULER
# MAGIC As a core component of data processing platform, scheduler is responsible for schedule tasks on compute units. Built on a Directed Acyclic Graph (DAG) compute model, Spark Scheduler works together with Block Manager and Cluster Backend to efficiently utilize cluster resources for high performance of various workloads. This talk dives into the technical details of the full lifecycle of a typical Spark workload to be scheduled and executed, and also discusses how to tune Spark scheduler for better performance.
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://hxquangnhat.files.wordpress.com/2015/03/scheduling.jpeg" alt="ConboBox" style="width: 200">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### NODE
# MAGIC are individual Machines within a `Cluster`
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://icon-library.com/images/server-icon/server-icon-24.jpg" alt="ConboBox" style="width: 200">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### CLUSTER
# MAGIC 
# MAGIC is a group of `Nodes`. In conceptual level, a `Cluster` is comprised of a `Driver` and multiple `Executors`.
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn-icons-png.flaticon.com/512/2620/2620591.png" alt="ConboBox" style="width: 100;heigth: 50">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### DRIVER
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://freecontent.manning.com/wp-content/uploads/bonaci_runtimeArch_01.png" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC The `Driver` is at the heart of the `Spark Architecture`. The `Driver` is the Machine in which the `Spark Application` runs. It is responsible for the following things -
# MAGIC 
# MAGIC 
# MAGIC * A) Running and maintaining information about the `Spark Application`.
# MAGIC 
# MAGIC * B) Analyzing, Distributing, and, Scheduling work across the `Executors`, i.e., while processing the `Spark Application` the `Driver` divides it into `Jobs`, `Stages`, and, `Tasks`. Since, `Tasks` are, in turn, assigned to specific `Slots` in an `Executor`, it is the `Driver’s` job to keep track of -
# MAGIC 
# MAGIC * C) If a `Driver` is to produce a result, like finding out how many records are there in a “Dataset” using a “count” operation, then “Driver” receives the result and make further decision based on the received result.
# MAGIC 
# MAGIC * D) Responding to user’s program.
# MAGIC 
# MAGIC NOTE: In a single `Databricks Cluster`, there will be only one `Driver`, regardless of the number of `Executors`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### SESION
# MAGIC 
# MAGIC * After Spark 2.x onwards , SparkSession serves as the entry point for all Spark Functionality
# MAGIC * All Functionality available with SparkContext are also available with SparkSession.
# MAGIC * However if someone prefers to use SparkContext , they can continue to do so
# MAGIC 
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://gankrin.org/wp-content/uploads/2019/09/Capture-300x241.jpg" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC **SparkContext:**
# MAGIC * Before Spark 2.x , SparkContext was the entry point of any Spark Application
# MAGIC * It is the Main channel to access all Spark Functionality
# MAGIC 
# MAGIC 
# MAGIC **HiveContext:**
# MAGIC * HiveContext is a Superset of SQLContext.
# MAGIC * It can do what SQLContext can PLUS many other things
# MAGIC * Additional features include the ability to write queries using the more complete HiveQL parser, access to Hive UDFs, and the ability to read data from Hive tables.
# MAGIC 
# MAGIC **SQLContext:**
# MAGIC - Spark SQLContext allows us to connect to different Data Sources to write or read data from them
# MAGIC - However it has limitations – when the Spark program ends or the Spark shell is closed, all links to the datasoruces are gone and will not be available in the next session.

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXECUTOR
# MAGIC 
# MAGIC In the simplest term, an `Executor` is an instance of a `Java Virtual Machine`, which is running a `Spark Application` and, is designed to receive instruction from `Driver`.
# MAGIC 
# MAGIC 
# MAGIC An `Executor` also provides an `Environment` in which “Tasks” can be run. Each “Executor” has a number of `Slots`. Each “Slot” can be assigned a `Task`. So, an `Executor` leverages the natural capabilities of a `Java Virtual Machine` to execute many `Threads`. Example- if an `Executor` has `4 Cores`, then there will be `4 Threads`, or, `4 Slots` for the `Driver` to assign `Tasks` to that `Executor`.
# MAGIC 
# MAGIC 
# MAGIC The `Executors` are responsible for carrying out the work assigned by the `Driver`. Each Executor is responsible for two things -
# MAGIC 
# MAGIC * A) Execute the codes assigned by the `Driver`.
# MAGIC * B) Report the `State of the Computation` back to the `Driver`.
# MAGIC 
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://i2.wp.com/www.mycloudplace.com/wp-content/uploads/2019/07/spark-components-300x243.png?resize=300%2C243" alt="ConboBox" style="width: 200">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### PARTITIONS
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://dezyre.gumlet.io/images/blog/how-data-partitioning-in-spark-helps-achieve-more-parallelism/image_52157759021626962420505.png?w=610&dpr=2.6" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC A `Partition` is a single slice of a larger `Dataset`. Apache Spark divides a large `Dataset` into no more than 128MB chunks of data.
# MAGIC 
# MAGIC * As Apache Spark processes those `Partitions`, it assigns `Tasks` to those `Partitions`. One `Task` will process one and only one `Partition`. This `One-to-One Relationship` between the `Task` and `Partition` is one of the means by which Apache Spark maintains its `Stability`, and, `Repeatability`.
# MAGIC 
# MAGIC * The actual size of each `Partition` and where the data gets split are decided by the `Driver`.
# MAGIC 
# MAGIC * The size and limitation of a `Partition` is controlled by a handful of `Configuration Option`. The initial size of `Partition` is partially adjustable with various `Configuration Options`. For all practical purposes, tweaking `Configuration Options` is not going to achieve any real significant performance gain in vast majority of scenarios.
# MAGIC 
# MAGIC * Each `Executor` will hold a chunk of the data to be processed. Each of these chunks is called a `Spark Partition`. A `Partition` is a `Collection of Rows` that sits on one Physical Machine in the `Cluster of Computers`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOBS
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://miro.medium.com/max/975/1*SRl-AlKlUBUN13pnWgee7g.png" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC The secret to Apache Spark’s performance is `Parallelism`. In the Databricks Environment, when a `Spark Application` is submitted to the `Driver`, it is going to sub-divide the `Spark Application` into its Natural Hierarchy of — `Jobs`, `Stages` and `Tasks` respectively, where one `Action` may create one, or, more `Jobs`.
# MAGIC 
# MAGIC Each `Parallelized Action` is referred to as a `Job`.
# MAGIC 
# MAGIC * A) Stage: Each `Job` is broken down into `Stages`, which is a `Set of Ordered Steps` that together accomplishes a `Job`
# MAGIC 
# MAGIC A `Stage` cannot be completed until all the `Tasks` of that `Stage` are completed. If any of the `Tasks` inside a `Stage` fails, the entire `Stage` fails.
# MAGIC 
# MAGIC One `Long-Running Task` can delay an entire `Stage` from completing.
# MAGIC 
# MAGIC The `Shuffle` between two `Stages` is one of the most expensive operations in Apache Spark.
# MAGIC 
# MAGIC * B) Task: `Tasks` are the `Smallest Unit of Work`. `Tasks` are created by the `Driver` and assigned a `Partition` of data to process. Then `Tasks` are assigned to `Slots` for Parallel Execution. Once started, each `Task` will fetch its assigned `Partition` from the original Data Source.

# COMMAND ----------

# MAGIC %md
# MAGIC ### SLOTS
# MAGIC 
# MAGIC represents the `Lowest Unit of Parallelization`. Example- if an `Executor` has `4 Cores`, then that `Executor` can only do `4 Tasks` at a time.
# MAGIC 
# MAGIC A `Slot` executes a “Set of Transformations” against a single “Partition” as directed by the “Driver”. A `Driver` picks a single `Partition`. For that selected `Partition`, the `Driver` creates a `Task`, and, assigns that `Task` to a `Slot` in an `Executor`. That `Slot` executes the operations, dictated to the `Task` by the `Driver`.
# MAGIC 
# MAGIC Apache Spark `Parallelizes` at two levels -
# MAGIC 
# MAGIC A) One is Splitting the work among `Executors`.
# MAGIC B) The other is the `Slot`. `Slots` are also interchangeably called as `Threads`, or, `Cores` in Databricks.
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://miro.medium.com/max/1400/1*cx6rG9vrz3nXM9xUkLWY7A.png" alt="ConboBox" style="width: 200">
# MAGIC </div>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## RDD
# MAGIC 
# MAGIC essential core &  intermediate transformations
# MAGIC 
# MAGIC 
# MAGIC * offer control and flexibility
# MAGIC * low level API & control datasets
# MAGIC * Type- safe
# MAGIC * Emcourage how-to
# MAGIC * dealing with unstrucrured data
# MAGIC * manipula data with lambda function than DSL ??
# MAGIC * don´t care schema or structure data
# MAGIC * sacrifice optimization, performance & inefficiecies
# MAGIC 
# MAGIC 
# MAGIC whats the problems
# MAGIC 
# MAGIC * Express how-to, not what-to
# MAGIC * no optimized by spark
# MAGIC * slow for non-JVM languages like Python
# MAGIC * inadvertent inefficiencies

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATAFRAMES
# MAGIC 
# MAGIC why
# MAGIC * high-level  APIS and DSL
# MAGIC * Strong type-safety
# MAGIC * easy of use & readability
# MAGIC * what-to-do
# MAGIC 
# MAGIC when
# MAGIC 
# MAGIC * Structured data schema
# MAGIC * code optimization and performance
# MAGIC * space efficiency with Tungsten

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATASETS 
