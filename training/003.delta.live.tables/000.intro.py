# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # DELTA LIVE TABLES
# MAGIC
# MAGIC Databricks Delta Live Tables is a Databricks functionality that allows users to work with real-time data in a scalable and efficient way. This functionality combines real-time stream processing technology with Databricks Delta Lake data management capabilities to provide a complete platform for real-time data processing.
# MAGIC
# MAGIC In Databricks Delta Live Tables, data is stored in tables that are optimized for real-time data access and manipulation. These tables are persistent and are automatically updated as new data is entered. Users can query and manipulate these tables using SQL, Python, R, or Scala, and the results are updated in real time as the underlying data changes.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## WORKFLOWS VS LIVE TABLES
# MAGIC
# MAGIC
# MAGIC Databricks Workflows and Databricks Delta Live Tables are two different Databricks functionalities that are used for different purposes. Here are the advantages and disadvantages of each of them:
# MAGIC
# MAGIC **Databricks Workflows**:
# MAGIC
# MAGIC **Advantages**:
# MAGIC
# MAGIC * It allows you to programmatically define, orchestrate, and execute data pipelines.
# MAGIC * Easily integrates with other Databricks tools such as Delta Lake and MLflow.
# MAGIC * It allows you to schedule tasks in a flexible way and schedule them based on events, schedules or external triggers.
# MAGIC
# MAGIC **Disadvantages**:
# MAGIC
# MAGIC * It requires programming knowledge and a certain level of skill in creating and managing data pipelines.
# MAGIC * It can be complex to set up and maintain if the pipelines are very complex.
# MAGIC
# MAGIC **Databricks Delta Live Tables**:
# MAGIC **Advantages**:
# MAGIC
# MAGIC * It provides an abstraction layer on top of the data stored in Delta Lake, making it easy to access and manipulate the data in real time.
# MAGIC * Allows integration with real-time data analysis and visualization tools, such as Apache Kafka and Tableau.
# MAGIC * It provides an intuitive user interface for querying and manipulating data in real time.
# MAGIC
# MAGIC **Disadvantages**:
# MAGIC
# MAGIC * It is not as flexible as Databricks Workflows, as it is specifically designed for real-time data manipulation and analysis.
# MAGIC * It is not the best choice for very complex data pipelines or for work that requires a high degree of orchestration and programming.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## OVERVIEW
# MAGIC [![Watch the video](https://i.postimg.cc/G2qCTz8P/dlt.png)](https://www.youtube.com/watch?v=MZd2MgM5JFY&t=952s&ab_channel=AdvancingAnalytics)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## EXPECTATIONS
# MAGIC You use expectations to define data quality constraints on the contents of a dataset. 
# MAGIC
# MAGIC [reference](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/expectations)
