-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # INTRO
-- MAGIC cuando trabajamos en databrics, la palabra database es un alias de schema, es decir que usar database o schema es lo mismo, pero schema en el ambito de databricks es mas apropiado

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CREATE SCHEMA
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ```sql
-- MAGIC CREATE [SCHEMA|DATABASE] [ IF NOT EXISTS ] schema_name
-- MAGIC     [ COMMENT schema_comment ]
-- MAGIC     [ LOCATION schema_directory ]
-- MAGIC     [ WITH DBPROPERTIES ( property_name = property_value [ , ... ] ) ]
-- MAGIC     }
-- MAGIC ```
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC by default, if you don't specify the database path, the scheme will be saved at `dbfs:/user/hive/warehouse/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE BASIC

-- COMMAND ----------

CREATE SCHEMA demo1;

-- COMMAND ----------

CREATE SCHEMA demo1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE IF NOT EXISTS

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE WITH COMMENTS

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo2
COMMENT 'database comment';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE WITH PROPERTIES

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo3
COMMENT 'database with properties'
WITH DBPROPERTIES (
  environment='dev',
  dbowner='luisvasv',
  dbversion='0.0.1',
  engine='azure'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CREATE WITH LOCATION

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo4
COMMENT 'external database location'
LOCATION '/mnt/bronze/databases/demo4'
WITH DBPROPERTIES (
  environment='dev',
  dbowner='luisvasv',
  dbversion='0.0.1',
  zone='bronze'
);

-- COMMAND ----------

-- MAGIC %fs ls /mnt/bronze/databases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SHOW SCHEMA
-- MAGIC 
-- MAGIC ```sql
-- MAGIC SHOW (SCHEMAS|DATABASES) [ LIKE regex_pattern ]
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SHOW SCHEMAS

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SHOW SCHEMAS LIKE

-- COMMAND ----------

SHOW SCHEMAS LIKE 'demo*'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DESCRIBE SCHEMA
-- MAGIC ```sql
-- MAGIC { DESC | DESCRIBE } (SCHEMA|DATABASE) [ EXTENDED ] schema_name
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DESCRIBE SIMPLE

-- COMMAND ----------

DESCRIBE SCHEMA demo1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DESCRIBE EXTENDED

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED test;

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED demo4;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ALTER SCHEMA
-- MAGIC 
-- MAGIC ```sql
-- MAGIC ALTER { SCHEMA | DATABASE schema_name
-- MAGIC    { SET DBPROPERTIES ( { key = val } [, ...] ) |
-- MAGIC      OWNER TO principal }
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DBPROPERTIES

-- COMMAND ----------

ALTER SCHEMA demo1 SET DBPROPERTIES ('action'='property set up using alter');
DESCRIBE SCHEMA EXTENDED demo1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### OWNER
-- MAGIC 
-- MAGIC in this seccion you should has active `access control`, please see:
-- MAGIC https://docs.microsoft.com/en-us/azure/databricks/security/access-control/table-acls/table-acl
-- MAGIC 
-- MAGIC if you doesn't have configured it, you will see and error like:
-- MAGIC ```bash
-- MAGIC Error in SQL statement: SparkException: Table Access Control is not enabled on this cluster.
-- MAGIC ```

-- COMMAND ----------

ALTER SCHEMA demo2 OWNER TO `luisvasv@gmail.com`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DROP SCHEMA
-- MAGIC 
-- MAGIC ```sql
-- MAGIC DROP (SCHEMA|DATABASE) [ IF EXISTS ] schema_name [ RESTRICT | CASCADE ]
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### GENERAL

-- COMMAND ----------

-- by default, this clausule use RESTRICT clausule, it means will restrict dropping a non-empty schema
DROP SCHEMA demo1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### IF EXISTS

-- COMMAND ----------

DROP SCHEMA IF EXISTS demo1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CASCADE

-- COMMAND ----------

-- will drop all the associated tables and functions.
DROP SCHEMA IF EXISTS demo2 CASCADE;
