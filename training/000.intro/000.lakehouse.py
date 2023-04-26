# Databricks notebook source
# MAGIC %md
# MAGIC # DATABRICKS LIKEHOUSE

# COMMAND ----------

# MAGIC %md
# MAGIC ## WHAT IS DB LAKEHOUSE?
# MAGIC 
# MAGIC A lakehouse is a new, open architecture that combines the best elements of data lakes and data warehouses. Lakehouses are enabled by a new system design: implementing similar data structures and data management features to those in a data warehouse directly on top of low cost cloud storage in open formats. They are what you would get if you had to redesign data warehouses in the modern world, now that cheap and highly reliable storage (in the form of object stores) are available.
# MAGIC 
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://www.databricks.com/wp-content/uploads/2020/01/data-lakehouse-new-1024x538.png" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC [reference](https://www.databricks.com/blog/2020/01/30/what-is-a-data-lakehouse.html)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## DB-LH COMPONENTS

# COMMAND ----------

# MAGIC %md 
# MAGIC ### DELTA TABLES
# MAGIC 
# MAGIC Tables created on Azure Databricks use the Delta Lake protocol by default. 
# MAGIC 
# MAGIC 
# MAGIC [reference](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/#delta-tables)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### ACID
# MAGIC Databricks uses Delta Lake by default for all reads and writes and builds upon the ACID guarantees provided by the open source Delta Lake protocol. ACID stands for atomicity, consistency, isolation, and durability.
# MAGIC 
# MAGIC * Atomicity means that all transactions either succeed or fail completely.
# MAGIC * Consistency guarantees relate to how a given state of the data is observed by simultaneous operations.
# MAGIC * Isolation refers to how simultaneous operations potentially conflict with one another.
# MAGIC * Durability means that committed changes are permanent.
# MAGIC 
# MAGIC [reference](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/acid)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### VERSIONING
# MAGIC 
# MAGIC Each operation that modifies a Delta Lake table creates a new table version. You can use history information to audit operations or query a table at a specific point in time.
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://delta.io/static/861c2993a8187b1a94fc99dee343885c/76823/image1.png" alt="ConboBox" style="width: 200">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC #### ETL
# MAGIC 
# MAGIC Databricks offers a variety of ways to help you load data into a lakehouse backed by Delta Lake.
# MAGIC 
# MAGIC 
# MAGIC * Auto Loader
# MAGIC * Delta Live Tables and Auto Loader
# MAGIC * Upload local data files or connect external data sources
# MAGIC * Load data into Azure Databricks using third-party tools
# MAGIC * COPY INTO
# MAGIC * Jobs
# MAGIC 
# MAGIC 
# MAGIC [reference](https://learn.microsoft.com/en-us/azure/databricks/ingestion/)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### INDEXING
# MAGIC Z-ordering is a technique to colocate related information in the same set of files. This co-locality is automatically used by Delta Lake on Azure Databricks data-skipping algorithms. This behavior dramatically reduces the amount of data that Delta Lake on Azure Databricks needs to read. 
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://delta.io/static/6df2054d3bb5f33e1fed25747a9e5ad2/52fbd/optimize-z-order-concept.png" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC [reference](https://learn.microsoft.com/en-us/azure/databricks/delta/data-skipping)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### UNIT CATALOG
# MAGIC 
# MAGIC Unity Catalog unifies data governance and discovery on Azure Databricks. Available in notebooks, jobs, and Databricks SQL, Unity Catalog provides features and UIs that enable workloads and users designed for both data lakes and data warehouse.
# MAGIC 
# MAGIC * Account-level management of the Unity Catalog metastore means databases, data objects, and permissions can be shared across Azure Databricks workspaces.
# MAGIC * You can leverage three tier namespacing `(<catalog>.<database>.<table>)` for organizing and granting access to data.
# MAGIC * External locations and storage credentials are also securable objects with similar ACL setting to other data objects.
# MAGIC * The Data Explorer provides a graphical user interface to explore databases and manage permissions.

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA GOBERNANCE
# MAGIC 
# MAGIC Data governance is the oversight to ensure that data brings value and supports your business strategy. Data governance encapsulates the policies and practices implemented to securely manage the data assets within an organization. 
# MAGIC 
# MAGIC [reference](https://learn.microsoft.com/en-us/azure/databricks/data-governance/)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA SHARING
# MAGIC 
# MAGIC Delta Sharing is an open protocol developed by Databricks for secure data sharing with other organizations regardless of the computing platforms they use. Azure Databricks builds Delta Sharing into its Unity Catalog data governance platform, enabling an Azure Databricks user, called a data provider, to share data with a person or group outside of their organization, called a data recipient.
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://learn.microsoft.com/en-us/azure/databricks/_static/images/delta-sharing/delta-sharing.png" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC [reference](https://learn.microsoft.com/en-us/azure/databricks/data-sharing/)

# COMMAND ----------

# MAGIC %md
# MAGIC #### AUDIT UNIT CATALOG EVENTS
# MAGIC Unity Catalog captures an audit log of actions performed against the metastore. This enables admins to access fine-grained details about who accessed a given dataset and the actions they performed.
# MAGIC 
# MAGIC [reference](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/audit)
