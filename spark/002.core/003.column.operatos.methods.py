# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ```
# MAGIC && , || & | boolean and, or scala| python
# MAGIC *, + ,-, <, >, >=, <= math comparison operators
# MAGIC === =!= / == , != equality and inequality tests in scala | python
# MAGIC alias as give the column an alias | only scala
# MAGIC cast, astype. cast the column to different data type , astype only python
# MAGIC isNull,isNotNull,isNan  
# MAGIC as,desc return a sort expression based on ascending/descending order of the column
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ```
# MAGIC select  : return a new dataframe
# MAGIC drop
# MAGIC withColumnRenamed
# MAGIC WithColumn
# MAGIC filter,where
# MAGIC sort,orderBy
# MAGIC dropDuplicates, distinct
# MAGIC limit
# MAGIC groupby
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC %md ## DataFrame Transformation Methods
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | select | Returns a new DataFrame by computing given expression for each element |
# MAGIC | drop | Returns a new DataFrame with a column dropped |
# MAGIC | withColumnRenamed | Returns a new DataFrame with a column renamed |
# MAGIC | withColumn | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | filter, where | Filters rows using the given condition |
# MAGIC | sort, orderBy | Returns a new DataFrame sorted by the given expressions |
# MAGIC | dropDuplicates, distinct | Returns a new DataFrame with duplicate rows removed |
# MAGIC | limit | Returns a new DataFrame by taking the first n rows |
# MAGIC | groupBy | Groups the DataFrame using the specified columns, so we can run aggregation on them |
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC rows
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ```
# MAGIC index
# MAGIC count
# MAGIC asDict
# MAGIC row.key
# MAGIC row["key"]
# MAGIC key in row
# MAGIC ```
