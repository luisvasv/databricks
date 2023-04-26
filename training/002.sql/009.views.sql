-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # VIEWS
-- MAGIC 
-- MAGIC A view is nothing more than a SQL statement that is stored in the database with an associated name. A view is actually a composition of a table in the form of a predefined SQL query.
-- MAGIC 
-- MAGIC A view can contain all rows of a table or select rows from a table. A view can be created from one or many tables which depends on the written SQL query to create a view.
-- MAGIC 
-- MAGIC Views, which are a type of virtual tables allow users to do the following −
-- MAGIC 
-- MAGIC * Structure data in a way that users or classes of users find natural or intuitive.
-- MAGIC 
-- MAGIC * Restrict access to the data in such a way that a user can see and (sometimes) modify exactly what they need and no more.
-- MAGIC 
-- MAGIC * Summarize data from various tables which can be used to generate reports.
-- MAGIC 
-- MAGIC [source](https://www.tutorialspoint.com/sql/sql-using-views.htm)
-- MAGIC 
-- MAGIC 
-- MAGIC ```sql
-- MAGIC 
-- MAGIC CREATE [ OR REPLACE ] [ [GLOBAL] TEMPORARY ] VIEW [ IF NOT EXISTS ] view_name
-- MAGIC     [ column_list ]
-- MAGIC     [ COMMENT view_comment ]
-- MAGIC     [ TBLPROPERTIES clause ]
-- MAGIC     AS query
-- MAGIC 
-- MAGIC column_list
-- MAGIC    ( { column_alias [ COMMENT column_comment ] } [, ...] )
-- MAGIC ```
-- MAGIC 
-- MAGIC 
-- MAGIC In Databricks we have 3 kind of views:
-- MAGIC 
-- MAGIC * **STORED**: a read-only object composed from one or more tables and views in a metastore (it's accesible like a table)
-- MAGIC * **GLOBAL TEMPORARY**:  they are linked to a temporary schema that is persisted in the system and is called global_temp. it is available to all Notebooks running on that Databricks Cluster and are discarded when the session ends
-- MAGIC * **TEMPORARY**: are only visible to the session that created them and are discarded when the session ends.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## CREATE STORED

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### SIMPLE

-- COMMAND ----------

CREATE VIEW IF NOT EXISTS test.v_dml_1 AS
  SELECT * FROM test.dml 
  WHERE tag = 'OK';

SELECT * FROM test.v_dml_1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### FORMATTED

-- COMMAND ----------

CREATE VIEW IF NOT EXISTS test.v_dml_2 (
  user_email COMMENT 'user email to send information',
  capital_district COMMENT 'information of country capital'
) COMMENT 'view formatted' 
  TBLPROPERTIES (
    project.info = 'training',
    project.version = '1.0'
  )
AS
  SELECT email, citydc FROM test.dml 
  WHERE tag = 'OK';

SELECT * FROM test.v_dml_2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## CREATE TEMPORARY
-- MAGIC 
-- MAGIC apply the some capabilities that stored views, but for being a temporal view is no required add details because remember that it will be removed automatically from databricks when your session expire
-- MAGIC 
-- MAGIC Exceptions:
-- MAGIC 
-- MAGIC * It is not allowed to define a TEMPORARY view with IF NOT EXISTS
-- MAGIC * only accept single-part view names (avoid schema)

-- COMMAND ----------

CREATE TEMPORARY VIEW v_dml_3 COMMENT 'temporary view' 
AS
  SELECT email, citydc FROM test.dml;

-- COMMAND ----------

select * from v_dml_3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## CREATE GLOBAL TEMPORARY
-- MAGIC 
-- MAGIC the global views are sotred on the temporal schema: `global_temp`
-- MAGIC Exceptions:
-- MAGIC 
-- MAGIC * It is not allowed to define a TEMPORARY view with IF NOT EXISTS
-- MAGIC * only accept single-part view names (avoid schema)

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW v_dml_4 COMMENT 'global temporary view' 
AS
  SELECT email FROM test.dml;

-- COMMAND ----------

select * from global_temp.v_dml_4;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## CREATE MATERIALIZED VIEW
-- MAGIC 
-- MAGIC Delta Pipelines also offers the ability to create a materialized view of the data by simply chaining the `.materialize()` operator on to the dataset.
-- MAGIC 
-- MAGIC This means that the “events” materialized view will be continuously updated as data flows through the Delta Pipeline. As new data arrives and is committed to the base table via ACID transactions, the materialized view is continuously updated incrementally for each of those transactions as well
-- MAGIC 
-- MAGIC 
-- MAGIC example:
-- MAGIC 
-- MAGIC ```python
-- MAGIC dataset("events")
-- MAGIC   .query {
-- MAGIC      input("base_table")
-- MAGIC      .select(...)
-- MAGIC      .filter(...)
-- MAGIC   }
-- MAGIC      .materialize()
-- MAGIC ```
-- MAGIC 
-- MAGIC [source](https://www.databricks.com/glossary/materialized-views)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## GET VIEW INFO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### SHOW VIEWS

-- COMMAND ----------

SHOW VIEWS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### SHOW SPECIFIC VIEW

-- COMMAND ----------

SHOW VIEWS LIKE 'v_dml_3'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### DESCRIBE

-- COMMAND ----------

describe test.v_dml_2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### DESCRIBE EXTENDED

-- COMMAND ----------

describe extended test.v_dml_2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## DROP
-- MAGIC 
-- MAGIC Removes the metadata associated with a specified view from the catalog. To drop a view you must be its owner.
-- MAGIC 
-- MAGIC `DROP VIEW [ IF EXISTS ] [schema.]view_name`
-- MAGIC 
-- MAGIC [source](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-drop-view)

-- COMMAND ----------

-- create demo view
CREATE VIEW IF NOT EXISTS test.v_alter AS
  SELECT 5 * 5 as test FROM test.dml;

-- COMMAND ----------

select * from test.v_alter

-- COMMAND ----------

DROP VIEW IF EXISTS test.v_alter;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## ALTER
-- MAGIC 
-- MAGIC Alters metadata associated with the view. It can change the definition of the view, change the name of a view to a different name, set and unset the metadata of the view by setting `TBLPROPERTIES`.
-- MAGIC 
-- MAGIC 
-- MAGIC ```
-- MAGIC ALTER VIEW view_name
-- MAGIC   { rename |
-- MAGIC     SET TBLPROPERTIES clause |
-- MAGIC     UNSET TBLPROPERTIES clause |
-- MAGIC     alter_body |
-- MAGIC     owner_to }
-- MAGIC 
-- MAGIC rename
-- MAGIC   RENAME TO to_view_name
-- MAGIC 
-- MAGIC alter_body
-- MAGIC   AS query
-- MAGIC 
-- MAGIC ```
-- MAGIC 
-- MAGIC [source](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-alter-view)

-- COMMAND ----------

DROP VIEW IF EXISTS test.v_alter;
DROP VIEW IF EXISTS test.v_modified;
CREATE VIEW IF NOT EXISTS test.v_alter AS
  SELECT email, citydc , 'V1' as harcoded FROM test.dml;

-- COMMAND ----------

select * from test.v_alter

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### RENAME

-- COMMAND ----------

ALTER VIEW test.v_alter RENAME TO test.v_modified;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### SET PROPERTIES

-- COMMAND ----------

ALTER VIEW test.v_modified SET TBLPROPERTIES (
  "app.ownser"="pepito",
  "product.ownner"="databricks",
  version=1
);

-- COMMAND ----------

describe table extended test.v_modified;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### UNSET PROPERTIES
-- MAGIC 
-- MAGIC you can unset properties in 3 ways:
-- MAGIC 
-- MAGIC *  \`some\`.\`value\`
-- MAGIC *  `'some.value'`
-- MAGIC *  some.value

-- COMMAND ----------

ALTER VIEW test.v_modified UNSET TBLPROPERTIES (
  `app`.`ownser`,
  "product.ownner",
  version
);

-- COMMAND ----------

describe table extended test.v_modified;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### QUERY

-- COMMAND ----------

ALTER VIEW test.v_modified AS
  SELECT email, citydc , 'V1' as harcoded, 1*1 as x FROM test.dml;

-- COMMAND ----------

select * FROM test.v_modified

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### OWNER

-- COMMAND ----------

 ALTER VIEW v1 OWNER TO `email@user`
