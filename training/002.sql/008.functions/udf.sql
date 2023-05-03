-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # UDF
-- MAGIC A user-defined function (UDF) is a function defined by a user, allowing custom logic to be reused in the user environment.
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://i.postimg.cc/13mFftd7/udf.png" alt="ConboBox" style="width: 200">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## UDF PERFORMANCE
-- MAGIC
-- MAGIC Depending on the language that you are using to create the UDF, the performance can be affected by multiple reasons such as:
-- MAGIC
-- MAGIC * **Python interpretation**: Python is an interpreted language, which means that Python code is executed line-by-line at runtime. On the other hand, Scala is a compiled language, which means that Scala code is compiled into binary code before execution. This difference in how the code is executed can affect the performance of Python UDFs.
-- MAGIC
-- MAGIC * **Global Interpreter Lock (GIL)**: Python uses the GIL to ensure that only one execution thread runs in the Python interpreter at any given time. This can limit the performance of Python UDFs in multi-core situations.
-- MAGIC
-- MAGIC * **Dynamic typing**: Python is a dynamically typed language, meaning that a variable's data type is not specified until a value is assigned to that variable at runtime. Scala, on the other hand, is a statically typed language, which means that the data type of a variable is specified at compile time. Dynamic typing can lead to the need for data type conversion at runtime, which can affect the performance of Python UDFs.
-- MAGIC
-- MAGIC
-- MAGIC * **Resources fighting**: Starting this Python process is expensive, but the real cost is in serializing the data to Python. This is costly for two reasons: it is an expensive computation, but also, after the data enters Python, Spark cannot manage the memory of the worker. This means that you could potentially cause a worker to fail if it becomes resource constrained (because both the JVM and Python are competing for memory on the same machine).
-- MAGIC
-- MAGIC You can check more details, visit this useful [link](https://mindfulmachines.io/blog/2018/6/apache-spark-scala-vs-java-v-python-vs-r-vs-sql26) 
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://images.squarespace-cdn.com/content/v1/565272dee4b02fdfadbb3d38/1528638115148-14NGI4JUR1SSKLGSVESF/Simply+UDF+Perf.png?format=750w" alt="ConboBox" style="width: 200">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CREATE
-- MAGIC
-- MAGIC
-- MAGIC ```
-- MAGIC CREATE [OR REPLACE] [TEMPORARY] FUNCTION [IF NOT EXISTS]
-- MAGIC     [metastore.schema.]function_name ( [ function_parameter [, ...] ] )
-- MAGIC     RETURNS { data_type | TABLE ( column_spec [, ...])
-- MAGIC     [ characteristic [...] ]
-- MAGIC     RETURN { expression | query }
-- MAGIC
-- MAGIC function_parameter
-- MAGIC     parameter_name data_type [DEFAULT default_expression] [COMMENT parameter_comment]
-- MAGIC
-- MAGIC column_spec
-- MAGIC     column_name data_type [COMMENT column_comment]
-- MAGIC
-- MAGIC characteristic
-- MAGIC   { LANGUAGE SQL |
-- MAGIC     [NOT] DETERMINISTIC |
-- MAGIC     COMMENT function_comment |
-- MAGIC     [CONTAINS SQL | READS SQL DATA] |
-- MAGIC     SQL SECURITY DEFINER }
-- MAGIC ```
-- MAGIC
-- MAGIC ---
-- MAGIC **PARAMETERS**
-- MAGIC
-- MAGIC
-- MAGIC * **OR REPLACE**: If specified, the function with the same name and signature (number of parameters and parameter types) is replaced
-- MAGIC * **TEMPORARY**: When you specify TEMPORARY, the created function is valid and visible in the current session. No persistent entry is made in the catalog.
-- MAGIC * **RETURNS DATA TYPE**:The return data type of the scalar function.
-- MAGIC * **RETURNS TABLE**: The signature of the result of the table function `(column name, data type, comment '')`
-- MAGIC * **RETURN EXPRESSION | QUERY**: For a scalar function, it can either be a query or an expression such as:
-- MAGIC     * [Aggregate functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin.html#aggregate-functions)
-- MAGIC     * [Window functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin.html#analytic-window-functions)
-- MAGIC     * [Ranking functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin.html#ranking-window-functions)
-- MAGIC     * [Row producing functions](https://docs.databricks.com/sql/language-manual/functions/explode.html)
-- MAGIC * **CHARACTERISTIC**: All characteristic clauses are optional. You can specify any number of them in any order, but you can specify each clause only once.
-- MAGIC     * LANGUAGE SQL: The language of the function. SQL is the only supported language.
-- MAGIC     * [NOT] DETERMINISTIC: Whether the function is deterministic. A function is deterministic when it returns only one result for a given set of arguments (tells us that the function will always return the same result set given the same arguments. ). The clause is for documentation only at this point. But at some point in the future it may be used to block non deterministic functions in certain contexts.
-- MAGIC     * COMMENT :A comment for the function. function_comment must be String literal.
-- MAGIC     * CONTAINS SQL or READS SQL DATA: Whether a function reads data directly or indirectly from a table or a view. When the function reads SQL data, you cannot specify CONTAINS SQL. If you don’t specify either clause, the property is derived from the function body. To sum up:
-- MAGIC
-- MAGIC
-- MAGIC         -  CONTAINS SQL tells us the function does not read or modify any data in a table. It is the default setting, so you normally wouldn’t specify it.
-- MAGIC
-- MAGIC
-- MAGIC     * SQL SECURITY DEFINER: The body of the function and any default expressions are executed using the authorization of the owner of the function. This is the only supported behavior.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL
-- MAGIC
-- MAGIC
-- MAGIC for a good practice, is recommended to user variables inside of the function in this way:
-- MAGIC
-- MAGIC ```
-- MAGIC CREATE FUNCTION demo(
-- MAGIC   name STRING COMMENT 'simple test'
-- MAGIC   .....
-- MAGIC
-- MAGIC     CONCATENATE(demo.name, 'S')
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### TEMPORAL

-- COMMAND ----------

CREATE TEMPORARY FUNCTION say_hello(
  name STRING COMMENT 'simple test'
) RETURNS STRING 
COMMENT 'a temporal UDF'
RETURN concat('training data engineer for  -->:', name);

-- COMMAND ----------

SELECT EXPLODE(ARRAY(1, 2, 3)) as ID, say_hello('demo') MSM1, say_hello('pepito') MSM2; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### PRESISTENT

-- COMMAND ----------

-- you can specify metastore and schema
CREATE OR REPLACE FUNCTION lha_dev.labs.to_hex(x INT COMMENT 'Any number between 0 - 255')
  RETURNS STRING
  COMMENT 'Converts a decimal to a hexadecimal'
  RETURN lpad(hex(least(greatest(0, x), 255)), 2, 0)

-- COMMAND ----------

SELECT lha_dev.labs.to_hex(id) FROM range(4);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DETERMINISTIC
-- MAGIC
-- MAGIC A function labeled as `DETERMINISTIC` returns the same result for a given set of input arguments each time it is called. In other words, if a function is deterministic, its result is predictable and consistent for the same input arguments. This means that if the same input is provided to the function multiple times, the function will return the same result each time.
-- MAGIC
-- MAGIC In this example, the UDF is labeled as `DETERMINISTIC` because it will always return the same result for the same input value. This means that the query processing system can optimize queries containing this function by reusing the results of the function for the same inputs.

-- COMMAND ----------

-- Create a DETERMINISTIC UDF that calculates the square of an input value
/*
In this example, the UDF is labeled as `DETERMINISTIC` because it will always return the same result for the same input value. This means that the query processing system can optimize queries containing this function by reusing the results of the function for the same inputs.
*/
CREATE OR REPLACE FUNCTION square(x DOUBLE COMMENT 'value to square') 
 RETURN DOUBLE
  CONTAINS SQL DETERMINISTIC
  COMMENT 'square a value'
RETURN x * x;

-- COMMAND ----------

SELECT square(2), square(5)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### NO DETERMINISTIC

-- COMMAND ----------

-- Create a DETERMINISTIC UDF that calculates the square of an input value
/*
In this example, the UDF is labeled as "NOT DETERMINISTIC" because it will return a different result each time it is called.
*/
CREATE OR REPLACE FUNCTION get_binary() 
 RETURNS DOUBLE
  CONTAINS SQL NOT DETERMINISTIC
  COMMENT 'get 0 or 1'
RETURN RAND();

-- COMMAND ----------

SELECT get_binary(), get_binary(), get_binary()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DEFAULT VALUE

-- COMMAND ----------

CREATE FUNCTION say_hello2(
  name STRING DEFAULT 'databricks' COMMENT 'simple test for default values'
) RETURNS STRING 
COMMENT 'default values'
RETURN concat('training data engineer for  -->:', name);

-- COMMAND ----------

SELECT EXPLODE(ARRAY(1, 2, 3)) as ID, say_hello2() MSM1, say_hello('abc') MSM2; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### LOOK UPS
-- MAGIC
-- MAGIC common usage of SQL UDF is to codify lookups. A simple lookup may be to decode some key, to understand the concep think about python dictionaries

-- COMMAND ----------

CREATE FUNCTION language_mapping_extension(extension STRING 
                             COMMENT 'a programing extension') 
   RETURNS STRING
   COMMENT 'decode key' 
   RETURN DECODE(extension, '.py', 'python',
                      '.java', 'Java');

-- COMMAND ----------

SELECT language_mapping_extension(".py")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### TABLE

-- COMMAND ----------

CREATE OR REPLACE FUNCTION demo_table(
  name STRING COMMENT 'student name', 
  elements ARRAY<INT> COMMENT 'elements'
)
RETURNS TABLE(x INT, y STRING)
    RETURN SELECT explode(demo_table.elements),  concat(demo_table.name, ' == ', 'OK')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### WAY #1

-- COMMAND ----------

SELECT demo_table.x, demo_table.y
FROM demo_table('db-table', ARRAY(1,2,3,4))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### WAY #2

-- COMMAND ----------

SELECT demo_table.*
FROM demo_table('db-table', ARRAY(1,2,3,4))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### WAY #3

-- COMMAND ----------

-- LATERAL correlation
SELECT demo_table.*
FROM VALUES (ARRAY(5,6)),
            (ARRAY(7,8)),
LATERAL demo_table('db-table', ARRAY(1,2,3,4))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### PYTHON

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### REQUIREMENTS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### CREATE DF

-- COMMAND ----------

-- MAGIC %python
-- MAGIC columns = ["id","full_name","delimeter_1", "delimeter_2"]
-- MAGIC data = [(1, "john jones", "***", "@@@"),
-- MAGIC     (2, "tracey smith", "***", "@@@"),
-- MAGIC     (3, "amy sanders", "***", "@@@")]
-- MAGIC
-- MAGIC df = spark.createDataFrame(data=data,schema=columns)
-- MAGIC df.show(truncate=False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### SPARK SIMPLE FUNCTION

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def concatenate(full_name, delimeter):
-- MAGIC   from pyspark.sql.functions import concat
-- MAGIC   return concat(delimeter, full_name, delimeter)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### CREATE NATIVE FUNCTION

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC def format_text(str):
-- MAGIC     return str.upper()[::-1]
-- MAGIC
-- MAGIC format_text("abc")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### CREATE FUNCTION DECORATOR

-- COMMAND ----------

-- MAGIC %python
-- MAGIC @udf(returnType=StringType()) 
-- MAGIC def format_tex_dec(str):
-- MAGIC     return str.upper()[::-1]

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CALL SPARK FUNCTION

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC display(
-- MAGIC     df.select(
-- MAGIC         concatenate(
-- MAGIC             col('full_name'),
-- MAGIC             col('delimeter_1')
-- MAGIC         ).alias("calculation")
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CALL FUNCTION DECORATOR

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC display(
-- MAGIC     df.select(
-- MAGIC         format_tex_dec(col('full_name')).alias("calculation2")
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CALL UDF

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### CREATE UDF

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import  udf
-- MAGIC from pyspark.sql.types import StringType
-- MAGIC
-- MAGIC udf_demo = udf(lambda name : format_text(name), StringType())

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### CALL UDF FUNCTION SIMPLE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC display(
-- MAGIC     df.select(
-- MAGIC         udf_demo(
-- MAGIC             col('full_name')
-- MAGIC         ).alias("calculation")
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### REGISTER
-- MAGIC
-- MAGIC to use the function on SQL, you need to register it

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.udf.register("db_training_udf", udf_demo)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CALLING FROM SQL

-- COMMAND ----------

SELECT db_training_udf("asdasdasdasdddddd")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## GET FUNCTION INFO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### EXPLAIN

-- COMMAND ----------

EXPLAIN SELECT demo_table.*
FROM demo_table('db-table', ARRAY(1,2))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DESCRIBE

-- COMMAND ----------

DESCRIBE FUNCTION say_hello2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DESCRIBE EXTENDED

-- COMMAND ----------

DESCRIBE  FUNCTION  EXTENDED say_hello2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SHOW FUNCTION

-- COMMAND ----------

SHOW FUNCTIONS say_hello2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SHOW FUNCTION LIKE

-- COMMAND ----------

SHOW FUNCTIONS LIKE 'say*'
