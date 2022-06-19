# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # TABLE OPERATION
# MAGIC 
# MAGIC 
# MAGIC tener montado algun proceso y crear notebook

# COMMAND ----------

# MAGIC %run ../../utilities/mount/with_storage_account $zone="bronze"

# COMMAND ----------

# MAGIC %run ../../utilities/mount/without_storage_account $zone="bronze"

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREATE TABLE
# MAGIC ```bash
# MAGIC { { [CREATE OR] REPLACE TABLE | CREATE TABLE [ IF NOT EXISTS ] }
# MAGIC   [SCHEMA] table_name
# MAGIC   [ column_specification ] [ USING data_source ]
# MAGIC   [ table_clauses ]
# MAGIC   [ AS query ] }
# MAGIC 
# MAGIC column_specification
# MAGIC   ( { column_identifier column_type [ NOT NULL ]
# MAGIC       [ GENERATED ALWAYS AS ( expr ) |
# MAGIC         GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( [ START WITH start ] [ INCREMENT BY step ] ) ] ]
# MAGIC       [ COMMENT column_comment ] } [, ...] )
# MAGIC 
# MAGIC table_clauses
# MAGIC   { OPTIONS clause |
# MAGIC     PARTITIONED BY clause |
# MAGIC     clustered_by_clause |
# MAGIC     LOCATION path [ WITH ( CREDENTIAL credential_name ) ] |
# MAGIC     COMMENT table_comment |
# MAGIC     TBLPROPERTIES clause } [...]
# MAGIC 
# MAGIC clustered_by_clause
# MAGIC   { CLUSTERED BY ( cluster_column [, ...] )
# MAGIC     [ SORTED BY ( { sort_column [ ASC | DESC ] } [, ...] ) ]
# MAGIC     INTO num_buckets BUCKETS }
# MAGIC     
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE TABLE DEFAULT

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE TABLE FROM SELECT

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE TABLE CLONE

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE TABLE WITH LIKE

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE TABLE WITH FORMAT

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE TABLE WITH LOCATION

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE TABLE WITH PROPERTIES

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE TABLE WITH PARTITION

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE TABLE WITH GENERATED COLUMN

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE TABLE WITH COMMENTS

# COMMAND ----------

# MAGIC %md
# MAGIC ### 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 
