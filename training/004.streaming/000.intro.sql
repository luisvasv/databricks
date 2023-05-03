-- Databricks notebook source
-- MAGIC %md
-- MAGIC # STREAMING

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## WHAT IS?
-- MAGIC
-- MAGIC Streaming refers to the delivery of real-time multimedia content over the internet. Instead of downloading a complete file before being able to view or listen to it, the user can access a live stream that is loaded as it is played.
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://hazelcast.com/wp-content/uploads/2021/12/21_Streaming_1-400x281-1.png" alt="ConboBox" style="width: 200">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## APACHE SPARK STREAMING
-- MAGIC
-- MAGIC Apache Spark Streaming is an extension of the Apache Spark data processing framework that allows processing of data in real-time through streams. This means that data is received and processed as it is generated, enabling faster decision-making and better responsiveness. Spark Streaming is capable of receiving data from various sources such as Kafka, Flume, and Twitter, and processing it in real-time using the distributed processing capabilities of Spark.
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://spark.apache.org/docs/latest/img/streaming-arch.png" alt="ConboBox" style="width: 200">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## APACHE SPARK MODES
-- MAGIC
-- MAGIC `outputMode()` describes what data is written to a data sink
-- MAGIC
-- MAGIC Note: for the `.format()`, check [output-sinks](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks)
-- MAGIC
-- MAGIC [reference](https://sparkbyexamples.com/spark/spark-streaming-outputmode/)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### APPEND
-- MAGIC
-- MAGIC OutputMode in which only the new rows in the streaming DataFrame/Dataset will be written to the sink.
-- MAGIC
-- MAGIC ```
-- MAGIC df.writeStream
-- MAGIC     .format("console")
-- MAGIC     .outputMode("append")
-- MAGIC     .start()
-- MAGIC     .awaitTermination()
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### COMPLETE
-- MAGIC OutputMode in which all the rows in the streaming DataFrame/Dataset will be written to the sink every time there are some updates.
-- MAGIC
-- MAGIC ```
-- MAGIC df.writeStream
-- MAGIC     .format("console")
-- MAGIC     .outputMode("complete")
-- MAGIC     .start()
-- MAGIC     .awaitTermination()
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UPDATE
-- MAGIC
-- MAGIC OutputMode in which only the rows that were updated in the streaming DataFrame/Dataset will be written to the sink every time there are some updates.
-- MAGIC
-- MAGIC Note: It is similar to the complete with one exception; update output mode outputMode("update") just outputs the updated aggregated results every time to data sink when new data arrives. but not the entire aggregated results like complete mode.
-- MAGIC
-- MAGIC ```
-- MAGIC df.writeStream
-- MAGIC     .writeStream
-- MAGIC     .format("console")
-- MAGIC     .outputMode("update")
-- MAGIC     .start()
-- MAGIC     .awaitTermination()
-- MAGIC ```
