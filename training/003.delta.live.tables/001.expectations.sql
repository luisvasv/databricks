-- Databricks notebook source
-- MAGIC %md 
-- MAGIC
-- MAGIC ## EXPECTATIONS
-- MAGIC Expectations are optional clauses you add to Delta Live Tables dataset declarations that apply data quality checks on each record passing through a query.
-- MAGIC
-- MAGIC ----
-- MAGIC
-- MAGIC An expectation consists of three things:
-- MAGIC
-- MAGIC
-- MAGIC * A description, which acts as a unique identifier and allows you to track metrics for the constraint.
-- MAGIC * A boolean statement that always returns true or false based on some stated condition.
-- MAGIC * An action to take when a record fails the expectation, meaning the boolean returns false.
-- MAGIC
-- MAGIC
-- MAGIC `CONSTRAINT constraint_name EXPECT (boolean logic) [ON VIOLATION [WARNING DEFAUT, FAIL UPDATE, DROP ROW, FAIL UPDATE]]`
-- MAGIC
-- MAGIC
-- MAGIC there are three actions you can apply to invalid records:
-- MAGIC
-- MAGIC
-- MAGIC * warn (default):	Invalid records are written to the target; failure is reported as a metric for the dataset.
-- MAGIC * drop	: Invalid records are dropped before data is written to the target; failure is reported as a metrics for the dataset.
-- MAGIC * fail	: Invalid records prevent the update from succeeding. Manual intervention is required before re-processing.
-- MAGIC
-- MAGIC
-- MAGIC [reference](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/expectations)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## DELTA LIVES TYPES
-- MAGIC
-- MAGIC * **DELTA LIVE TABLES**: In a Delta Live Table, data is stored on disk and is updated in real time as new data is entered. Users can query and manipulate this table using SQL, Python, R, or Scala, and the results are updated in real time as the underlying data changes. In addition, Delta Live Tables offer advanced data management features, such as integration with Delta Lake and the ability to process large volumes of data in real time with low latency.
-- MAGIC
-- MAGIC * **DELTA LIVE VIEW**:a Delta Live View is a view that is created from an existing Delta Live Table, allowing users to query and manipulate the data more efficiently. In a Delta Live View, data is read directly from the underlying Delta Live Table and any necessary transformations are applied to produce the view. As a result, Delta Live Views are lighter than Delta Live Tables and can be efficiently updated based on changes to the underlying Delta Live Table.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ## SQL 
-- MAGIC
-- MAGIC you can create delta live tables using sql sintaxis, and also provide differents constraints to make sure the data quality

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ### WARN

-- COMMAND ----------

CONSTRAINT valid_timestamp EXPECT (timestamp > '2012-01-01')

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ### DROP

-- COMMAND ----------

CONSTRAINT valid_current_page EXPECT (age > 0 AND name IS NOT NULL) ON VIOLATION DROP ROW

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ### FAIL

-- COMMAND ----------

CONSTRAINT valid_count EXPECT (count > 0) ON VIOLATION FAIL UPDATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## PYTHON
-- MAGIC
-- MAGIC Delta Live Tables Python functions are defined in the dlt module. Your pipelines implemented with the Python API must import this module:
-- MAGIC
-- MAGIC `import dlt`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### @dlt.table
-- MAGIC
-- MAGIC delta live table definition
-- MAGIC
-- MAGIC Note: you can define dlt.table without argments, will take the function name
-- MAGIC ```
-- MAGIC @dlt.table
-- MAGIC def demo():
-- MAGIC   pass
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %python
-- MAGIC @dlt.table(
-- MAGIC   name="<name>",
-- MAGIC   comment="<comment>",
-- MAGIC   spark_conf={"<key>" : "<value", "<key" : "<value>"},
-- MAGIC   table_properties={"<key>" : "<value>", "<key>" : "<value>"},
-- MAGIC   path="<storage-location-path>",
-- MAGIC   partition_cols=["<partition-column>", "<partition-column>"],
-- MAGIC   schema="schema-definition",
-- MAGIC   temporary=False)
-- MAGIC def demo():
-- MAGIC     pass

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### @dlt.view
-- MAGIC
-- MAGIC delta live view definition

-- COMMAND ----------

-- MAGIC %python
-- MAGIC @dlt.view(
-- MAGIC   name="<name>",
-- MAGIC   comment="<comment>")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### @dlt.expect
-- MAGIC
-- MAGIC
-- MAGIC `@expect(“description”, “constraint”)`
-- MAGIC
-- MAGIC Declare a data quality constraint identified by description. If a row violates the expectation, include the row in the target dataset.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC @dlt.expect_or_fail("valid_count", "count > 0")
-- MAGIC def demo():
-- MAGIC     pass

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### @expect_or_drop
-- MAGIC
-- MAGIC `@expect_or_drop(“description”, “constraint”)`
-- MAGIC
-- MAGIC Declare a data quality constraint identified by description. If a row violates the expectation, drop the row from the target dataset.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC @dlt.expect_or_drop("valid_count", "count > 0")
-- MAGIC def demo():
-- MAGIC     pass

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### @dlt.expect_or_fail
-- MAGIC
-- MAGIC `@expect_or_fail(“description”, “constraint”)`
-- MAGIC
-- MAGIC Declare a data quality constraint identified by
-- MAGIC description. If a row violates the expectation, immediately stop execution.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC @dlt.expect_or_fail("valid_count", "count > 0")
-- MAGIC def demo():
-- MAGIC     pass

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### @expect_all
-- MAGIC
-- MAGIC `@expect_all(expectations)`
-- MAGIC
-- MAGIC Declare one or more data quality constraints.
-- MAGIC `expectations` is a Python dictionary, where the key is the expectation description and the value is the expectation constraint. If a row violates any of the expectations, include the row in the target dataset.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC expectations: dict = {
-- MAGIC     "valid_count": "count > 0",
-- MAGIC     "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"
-- MAGIC }
-- MAGIC
-- MAGIC @dlt.expect_all(expectations)
-- MAGIC def demo():
-- MAGIC     pass

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### @expect_all_or_drop
-- MAGIC
-- MAGIC `@expect_all_or_drop(expectations)`
-- MAGIC
-- MAGIC Declare one or more data quality constraints. `expectations` is a Python dictionary, where the key is the expectation description and the value is the expectation constraint. If a row violates any of the expectations, drop the row from the target dataset.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC expectations: dict = {
-- MAGIC     "valid_count": "count > 0",
-- MAGIC     "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"
-- MAGIC }
-- MAGIC
-- MAGIC @dlt.expect_all_or_drop(expectations)
-- MAGIC def demo():
-- MAGIC     pass

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### @expect_all_or_fail
-- MAGIC
-- MAGIC `@expect_all_or_fail(expectations)`
-- MAGIC
-- MAGIC Declare one or more data quality constraints.
-- MAGIC `expectations` is a Python dictionary, where the key is the expectation description and the value is the expectation constraint. If a row violates any of the expectations, immediately stop execution.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC expectations: dict = {
-- MAGIC     "valid_count": "count > 0",
-- MAGIC     "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"
-- MAGIC }
-- MAGIC
-- MAGIC @dlt.expect_all_or_fail(expectations)
-- MAGIC def demo():
-- MAGIC     pass
