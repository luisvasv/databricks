-- Databricks notebook source
-- MAGIC %md What if the table wasn't registered in the metastore already? Let's take a directory of Parquet files and read into Delta.
-- MAGIC 
-- MAGIC Note: we're simplifying here for demostration purposes. We'd first want to mount a data store like S3 or ADLS to a directory called `intro-data`.

-- COMMAND ----------

CREATE TABLE customers USING PARQUET OPTIONS (path '/intro-data/')
CONVERT TO DELTA customers


-- COMMAND ----------

-- MAGIC %md What about another common use case: create table as (CTAS) statements? I'll create a table and insert some values first:

-- COMMAND ----------

CREATE TABLE students (name VARCHAR(64), street_address VARCHAR(64), student_id INT)
  USING DELTA 

-- COMMAND ----------

INSERT INTO students VALUES
    ('Issac Newton', 'Main Ave', 3145),
    ('Ada Lovelace', 'Not Main Ave', 2718);

-- COMMAND ----------

-- MAGIC %md Now, we could create a new table:

-- COMMAND ----------

CREATE TABLE main_street AS 
SELECT * FROM students
WHERE street_address = 'Main Ave'

-- COMMAND ----------

SELECT * 
FROM main_street

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Bonus! 
-- MAGIC Check out what's inside of a Delta Transaction log. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os, pprint
-- MAGIC path = "/dbfs/user/kevin.coyle@databricks.com/gym/mac_logs/_delta_log/00000000000000000002.json" 
-- MAGIC with open(path, "r") as file:
-- MAGIC   text = file.read()
-- MAGIC pprint.pprint(text)b

-- COMMAND ----------

-- Let's clean up the tables we created!
DROP TABLE test;

-- COMMAND ----------

DROP TABLE students;

-- COMMAND ----------

DROP TABLE main_street
