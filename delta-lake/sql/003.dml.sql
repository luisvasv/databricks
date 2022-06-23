-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DML

-- COMMAND ----------

USE test;
CREATE TABLE IF NOT EXISTS dml(
email STRING,
city STRING,
active TINYINT,
citydc STRING
) LOCATION '/mnt/bronze/table_dml'
PARTITIONED BY (active);

INSERT INTO dml (email,city,active,citydc) VALUES ('purus.gravida@aol.couk','Mmabatho',1,'Körfez');
INSERT INTO dml (email,city,active,citydc) VALUES ('lacinia.vitae@icloud.org','Linköping',0,'Manaure');
INSERT INTO dml (email,city,active,citydc) VALUES ('lacus@outlook.edu','Ponte nelle Alpi',1,'Périgueux');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SELECT

-- COMMAND ----------

SELECT *, input_file_name(),input_file_block_length() FROM dml;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## INSERT
-- MAGIC 
-- MAGIC ```
-- MAGIC INSERT { OVERWRITE | INTO } [ TABLE ] table_name
-- MAGIC     [ PARTITION clause ]
-- MAGIC     [ ( column_name [, ...] ) ]
-- MAGIC     query
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SINGLE ROW

-- COMMAND ----------

INSERT INTO dml (email,city,active,citydc) VALUES ('aa@icloud.org','vv',0,'Manaure');  -- column list
INSERT INTO dml VALUES ('bb@icloud.org','xx',0,'Manaure');                             -- general

-- COMMAND ----------

SELECT *, input_file_name() FROM dml;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### MULTI ROW

-- COMMAND ----------

INSERT INTO dml (email,city,active,citydc) VALUES 
('xx@icloud.org','aa',1,'medellin'),
('yy@icloud.org','bb',1,'medellin'),
('zz@icloud.org','cc',1,'medellin');

-- COMMAND ----------

SELECT *, input_file_name() FROM dml;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### PARTITION

-- COMMAND ----------

INSERT INTO dml PARTITION(active=3)
SELECT email,city,citydc FROM dml

-- COMMAND ----------

SELECT *, input_file_name() FROM dml;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### PARTITION & COLUMN LIST

-- COMMAND ----------

INSERT INTO dml PARTITION(active=7) (email,city,citydc) VALUES
('omg@icloud.org','cc','medellin')

-- COMMAND ----------

SELECT *, input_file_name() FROM dml WHERE active=7;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### TABLE CLAUSE

-- COMMAND ----------

CREATE TABLE clone_dml_v1 
DEEP CLONE dml VERSION AS OF 1 
LOCATION '/mnt/bronze/clone_dml_v1';

-- COMMAND ----------

SELECT * FROM clone_dml_v1;

-- COMMAND ----------

INSERT INTO clone_dml_v1  VALUES ('table@clause.edu','demo',5,'medellin');
INSERT INTO clone_dml_v1  VALUES ('delta@clause.edu','databricks',5,'medellin');

-- COMMAND ----------

SELECT * FROM clone_dml_v1;

-- COMMAND ----------

INSERT INTO dml TABLE clone_dml_v1;

-- COMMAND ----------

SELECT DISTINCT * FROM dml WHERE active=5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DIRECTORY

-- COMMAND ----------

INSERT INTO delta.`/mnt/bronze/table_dml` VALUES
('Amy Smith', 'directory', 9, 'San Jose');

-- COMMAND ----------

SELECT * FROM dml WHERE active=9;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### OVERWRITE
-- MAGIC 
-- MAGIC works exaclty the examples showed above, the unique diference, is that remove all data and insert newer to partition level o table level.
-- MAGIC 
-- MAGIC You must replace `INTO` BY `OVERWRITE`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## UPDATE
-- MAGIC 
-- MAGIC Updates the column values for the rows that match a predicate
-- MAGIC 
-- MAGIC ```
-- MAGIC UPDATE table_name [table_alias]
-- MAGIC    SET  { { column_name | field_name }  = expr } [, ...]
-- MAGIC    [WHERE clause]
-- MAGIC ```
-- MAGIC **NOTE**: In `SET` clause, you can define multiple columns separated by  `,`

-- COMMAND ----------

UPDATE dml
SET email='colombia.antioquia@org.com'
WHERE citydc = 'medellin';

-- COMMAND ----------

SELECT * FROM dml WHERE citydc = 'medellin';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DELETE
-- MAGIC Deletes the rows that match a predicate.
-- MAGIC 
-- MAGIC ```
-- MAGIC DELETE FROM table_name [table_alias] [WHERE predicate]
-- MAGIC ```

-- COMMAND ----------

DELETE FROM dml WHERE active=9;

-- COMMAND ----------

SELECT * FROM dml WHERE active = 9;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## MERGE INTO
-- MAGIC 
-- MAGIC `MERGE INTO` will merge a set of updates, insertions, and deletions based on a source table into a target Delta table.
-- MAGIC 
-- MAGIC ```
-- MAGIC MERGE INTO target_table_name [target_alias]
-- MAGIC    USING source_table_reference [source_alias]
-- MAGIC    ON merge_condition
-- MAGIC    [ WHEN MATCHED [ AND condition ] THEN matched_action ] [...]
-- MAGIC    [ WHEN NOT MATCHED [ AND condition ]  THEN not_matched_action ] [...]
-- MAGIC 
-- MAGIC matched_action
-- MAGIC  { DELETE |
-- MAGIC    UPDATE SET * |
-- MAGIC    UPDATE SET { column1 = value1 } [, ...] }
-- MAGIC 
-- MAGIC not_matched_action
-- MAGIC  { INSERT * |
-- MAGIC    INSERT (column1 [, ...] ) VALUES (value1 [, ...])
-- MAGIC ```
-- MAGIC **NOTE**: This statement is supported only for Delta Lake tables.
-- MAGIC 
-- MAGIC for a better understanding  we will create and table named `dml_merge` and to do alter table on ` dml`  and add `tag` column. 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dml_merge(
email STRING,
city STRING,
active TINYINT,
citydc STRING
) LOCATION '/mnt/bronze/table_dml_merge'
PARTITIONED BY (active);

INSERT INTO dml_merge (email,city,active,citydc) VALUES ('purus.gravida@aol.couk','Mmabatho',1,'Körfez');
INSERT INTO dml_merge (email,city,active,citydc) VALUES ('lacinia.vitae@icloud.org','Linköping',3,'Manaure');
INSERT INTO dml_merge (email,city,active,citydc) VALUES ('lacus@outlook.edu','Ponte nelle Alpi',5,'Périgueux');
INSERT INTO dml_merge (email,city,active,citydc) VALUES ('lacus@outlook.edu','Ponte nelle Alpi',7,'Périgueux');
INSERT INTO dml_merge (email,city,active,citydc) VALUES ('lacus@outlook.edu','Ponte nelle Alpi',50,'Périgueux');
INSERT INTO dml_merge (email,city,active,citydc) VALUES ('lacus@outlook.edu','Ponte nelle Alpi',60,'Périgueux');

-- COMMAND ----------

ALTER TABLE dml ADD COLUMN tag STRING;

-- COMMAND ----------

DESCRIBE TABLE dml;

-- COMMAND ----------

SELECT active, COUNT(1) counter
FROM dml
GROUP BY active;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC is important to unserstand that the first filter is a `JOIN` where the data doing mistmatch, will do the correspondent update,delete or insert
-- MAGIC 
-- MAGIC 
-- MAGIC | active | actions              |
-- MAGIC |--------|----------------------|
-- MAGIC | 1      | tag=OK               |
-- MAGIC | 3      | tag=OK               |
-- MAGIC | 5      | tag=OK               |
-- MAGIC | 7      | tag=OK               |
-- MAGIC | 0      | no actions           |
-- MAGIC | 10+    | insert;tag=OUT_RANGE |

-- COMMAND ----------

MERGE INTO dml objetive
  USING dml_merge source 
    ON objetive.active = source.active -- is possible add AND conditions AND logs.date > current_date() - INTERVAL 7 DAYS for example
  WHEN MATCHED AND objetive.active < 10 THEN 
    UPDATE SET objetive.tag='OK'
  WHEN NOT MATCHED THEN 
    INSERT(objetive.email, objetive.city, objetive.active, objetive.citydc, objetive.tag)
    VALUES (source.email,source.city,source.active,source.citydc,'OUT_RANGE')
 ;

-- COMMAND ----------

SELECT active,tag, COUNT(1) counter
FROM dml
GROUP BY active,tag

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DELTA

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DESCRIBE HISTORY
-- MAGIC 
-- MAGIC Returns provenance information, including the operation, user, and so on, for each write to a table. Table history is retained for 30 days.
-- MAGIC 
-- MAGIC **COLUMNS**
-- MAGIC 
-- MAGIC | Column              | Type      | Description                                                                |
-- MAGIC |---------------------|-----------|----------------------------------------------------------------------------|
-- MAGIC | version             | long      | Table version generated by the operation.                                  |
-- MAGIC | timestamp           | timestamp | When this version was committed.                                           |
-- MAGIC | userId              | string    | ID of the user that ran the operation.                                     |
-- MAGIC | userName            | string    | Name of the user that ran the operation.                                   |
-- MAGIC | operation           | string    | Name of the operation.                                                     |
-- MAGIC | operationParameters | map       | Parameters of the operation (for example, predicates.)                     |
-- MAGIC | job                 | struct    | Details of the job that ran the operation.                                 |
-- MAGIC | notebook            | struct    | Details of notebook from which the operation was run.                      |
-- MAGIC | clusterId           | string    | ID of the cluster on which the operation ran.                              |
-- MAGIC | readVersion         | long      | Version of the table that was read to perform the write operation.         |
-- MAGIC | isolationLevel      | string    | Isolation level used for this operation.                                   |
-- MAGIC | isBlindAppend       | boolean   | Whether this operation appended data.                                      |
-- MAGIC | operationMetrics    | map       | Metrics of the operation (for example, number of rows and files modified.) |
-- MAGIC | userMetadata        | string    | User-defined commit metadata if it was specified                           |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### TABLE

-- COMMAND ----------

DESCRIBE HISTORY dml;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### PATH

-- COMMAND ----------

DESCRIBE HISTORY '/mnt/bronze/table_dml';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DELTA PATH

-- COMMAND ----------

DESCRIBE HISTORY delta.`/mnt/bronze/table_dml`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### LAST OPERATION

-- COMMAND ----------

DESCRIBE HISTORY dml LIMIT 1  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### READ DELTA LOG

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df = spark.read.format("json").load("/mnt/bronze/table_dml/_delta_log/*.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## TIME TRAVEL
-- MAGIC 
-- MAGIC We can use a query an old snapshot of a table using time travel. Time travel is a specialized read on our dataset which allows us to read previous versions of our data. There are several ways to go about this. 
-- MAGIC 
-- MAGIC The `@` symbol can be used with a version number, aliased to `v#`, like the syntax below.
-- MAGIC 
-- MAGIC The `@` symbol can also be used with a version number or a datestamp in the format: `yyyyMMddHHmmssSSS`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### @V#

-- COMMAND ----------

SELECT * FROM dml@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### VERSION AS OF #

-- COMMAND ----------

SELECT * FROM dml VERSION AS OF 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OPTIMIZE
-- MAGIC 
-- MAGIC Optimizes the layout of Delta Lake data. Optionally optimize a subset of data or colocate data by column. If you do not specify colocation, bin-packing optimization is performed.
-- MAGIC 
-- MAGIC ```
-- MAGIC OPTIMIZE table_name [WHERE predicate]
-- MAGIC     [ZORDER BY (col_name1 [, ...] ) ]
-- MAGIC ```
-- MAGIC 
-- MAGIC **OPTIMIZATION ALGORITHMS**
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC * **bin-packing:** improve this speed is to coalesce small files into larger ones.
-- MAGIC 
-- MAGIC * **z-Ordering:** Z-Ordering is a technique to colocate related information in the same set of files. This co-locality is automatically used by Delta Lake on Databricks data-skipping algorithms. This behavior dramatically reduces the amount of data that Delta Lake on Databricks needs to read.
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC **ANOTATIONS**
-- MAGIC 
-- MAGIC * **WHERE:** optimize the subset of rows matching the given partition predicate. Only filters involving partition key attributes are supported.
-- MAGIC 
-- MAGIC * **ZORDER BY:** colocate column information in the same set of files. Co-locality is used by Delta Lake data-skipping algorithms to dramatically reduce the amount of data that needs to be read. You can specify multiple columns for ZORDER BY as a comma-separated list. However, the effectiveness of the locality drops with each additional column.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### GENERAL

-- COMMAND ----------

SELECT *, input_file_name() FROM dml

-- COMMAND ----------

OPTIMIZE dml;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### WHERE

-- COMMAND ----------

OPTIMIZE dml WHERE active = 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ZORDER

-- COMMAND ----------

OPTIMIZE dml 
ZORDER BY (citydc);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## BLOOM FILTER INDEX
-- MAGIC 
-- MAGIC 
-- MAGIC ## Bloom Filter Indexes
-- MAGIC 
-- MAGIC While Z-order provides useful data clustering for high cardinality data, it's often most effective when working with queries that filter against continuous numeric variables.
-- MAGIC 
-- MAGIC Bloom filters provide an efficient algorithm for probabilistically identifying files that may contain data using fields containing arbitrary text. Appropriate fields would include hashed values, alphanumeric codes, or free-form text fields.
-- MAGIC 
-- MAGIC Bloom filters calculate indexes that indicate the likelihood a given value **could** be in a file; the size of the calculated index will vary based on the number of unique values present in the field being indexed and the configured tolerance for false positives.
-- MAGIC 
-- MAGIC **NOTE**: A false positive would be a file that the index thinks could have a matching record but does not. Files containing data matching a selective filter will never be skipped; false positives just mean that extra time was spent scanning files without matching records.
-- MAGIC 
-- MAGIC Looking at the distribution for the `key` field, this is an ideal candidate for this technique.
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC 
-- MAGIC is a space-efficient data structure that enables data skipping on chosen columns, particularly for fields containing arbitrary text.
-- MAGIC 
-- MAGIC 
-- MAGIC https://docs.databricks.com/delta/optimizations/bloom-filters.html
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC %sql
-- MAGIC CREATE BLOOMFILTER INDEX
-- MAGIC ON TABLE date_part_table
-- MAGIC FOR COLUMNS(key OPTIONS (fpp=0.1, numItems=200))

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## AUTO OPTIMIZE
-- MAGIC 
-- MAGIC https://docs.databricks.com/delta/optimizations/auto-optimize.html

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##  VACUUM
-- MAGIC 
-- MAGIC cleans up files associated with a table. There are different versions of this command for Delta and Apache Spark tables.
-- MAGIC 
-- MAGIC **IMPORTANT:** If you run `VACUUM` on a Delta table, you lose the ability to time travel back to a version older than the specified data retention period.
-- MAGIC 
-- MAGIC `VACUUM table_name [RETAIN num HOURS] [DRY RUN]`
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC * **RETAIN num HOURS** : the retention threshold.
-- MAGIC 
-- MAGIC * **DRY RUN**: return a list of files to be deleted
-- MAGIC 
-- MAGIC 
-- MAGIC **DATABRICKS RECOMENDATION**
-- MAGIC 
-- MAGIC It is recommended that you set a retention interval to be at least 7 days, because old snapshots and uncommitted files can still be in use by concurrent readers or writers to the table. If `VACUUM` cleans up active files, concurrent readers can fail or, worse, tables can be corrupted when `VACUUM` deletes files that have not yet been committed. You must choose an interval that is longer than the longest running concurrent transaction and the longest period that any stream can lag behind the most recent update to the table.

-- COMMAND ----------

VACUUM dml RETAIN 168 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###  VACUUM FOR CLONED TABLES

-- COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
spark.sql("VACUUM sensors_prod RETAIN 0 HOURS")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###  VACUUM FOR TBLPROPERTIES

-- COMMAND ----------

ALTER TABLE sensors_backup
SET TBLPROPERTIES (
  delta.logRetentionDuration = '3650 days',
  delta.deletedFileRetentionDuration = '3650 days'
)
