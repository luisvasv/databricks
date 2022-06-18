# Databricks notebook source
# MAGIC %md
# MAGIC # WIDGETS

# COMMAND ----------

# MAGIC %md
# MAGIC ## HELP

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## PYTHON

# COMMAND ----------

# MAGIC %md
# MAGIC ### COMBOBOX
# MAGIC 
# MAGIC A combo box is a GUI feature that combines a drop-down box, list box, and/or an editable text field, giving the user multiple ways to input or select the desired information.
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://static.thenounproject.com/png/153641-200.png" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC `combobox(name: String, defaultValue: String, choices: Seq, label: String): void `

# COMMAND ----------

`combobox(name: String, defaultValue: String, choices: Seq, label: String): void `%md 
#### WITH DEFAULT VALUE

# COMMAND ----------

dbutils.widgets.combobox(
    name="demo_combobox_yd",
    defaultValue="delta",
    choices=["databricks", "spark", "delta"],
    label="combobox_default_value"
    )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITHOUT DEFAULT VALUE

# COMMAND ----------

dbutils.widgets.combobox(
    name="demo_combobox_nd",
    defaultValue="",
    choices=["databricks", "spark", "delta"],
    label="combobox_no_default_value"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROPDOWN
# MAGIC 
# MAGIC A drop down menu is horizontal list of options that each contain a vertical menu
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://static.thenounproject.com/png/3547958-200.png" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC `dropdown(name: String, defaultValue: String, choices: Seq, label: String): void`

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITH DEFAULT VALUE

# COMMAND ----------

dbutils.widgets.dropdown(
    name="demo_dropdown_yd",
    defaultValue="delta",
    choices=["databricks", "spark", "delta"],
    label="dropdown_default_value"
    )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITHOUT DEFAULT VALUE

# COMMAND ----------

dbutils.widgets.dropdown(
    name="demo_dropdown_yd",
    defaultValue="delta",
    choices=["databricks", "spark", "delta"],
    label="dropdown_default_value"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### MULTISELECT
# MAGIC 
# MAGIC 
# MAGIC Multi select dropdown list is used when a user wants to store multiple values for the same record, whereas dropdown list is used to store a single value for a record.
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://static.thenounproject.com/png/139096-200.png" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC `multiselect(name: String, defaultValue: String, choices: Seq, label: String): void`

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITH DEFAULT VALUE

# COMMAND ----------

dbutils.widgets.multiselect(
    name="vdemo_multiselect_yd",
    defaultValue="delta",
    choices=["databricks", "spark", "delta", "sparksql"],
    label="multiselect_default_value"
    )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITHOUT DEFAULT VALUE

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC 
# MAGIC ```
# MAGIC # date : 17/06/2022 
# MAGIC # no puede ser creado sin valores por defecto, si se crea se vera un error tal como:
# MAGIC 
# MAGIC com.databricks.dbutils_v1.DefaultValueNotInChoicesList: Selection sequence must include 
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### TEXT
# MAGIC 
# MAGIC Text fields allow users to enter text into a UI. They typically appear in forms and dialogs.
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://static.thenounproject.com/png/756265-200.png" alt="ConboBox" style="width: 200">
# MAGIC </div>
# MAGIC 
# MAGIC `text(name: String, defaultValue: String, label: String): void`

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITH DEFAULT VALUE

# COMMAND ----------

dbutils.widgets.text(
    name="vdemo_text_yd",
    defaultValue="TEXT ..",
    label="text_default_value"
    )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITHOUT DEFAULT VALUE

# COMMAND ----------

dbutils.widgets.text(
    name="demo_text_nd",
    defaultValue="",
    label="text_no_value"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### GET
# MAGIC 
# MAGIC  get current value of an input widget
# MAGIC  
# MAGIC  
# MAGIC `get(name: String): String -> retrieves widget value (s)`

# COMMAND ----------

# MAGIC %md 
# MAGIC #### MULTISELECT

# COMMAND ----------

dbutils.widgets.get("vdemo_multiselect_yd")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### TEXT

# COMMAND ----------

dbutils.widgets.get("vdemo_text_yd")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### DROPDOWN

# COMMAND ----------

dbutils.widgets.get("demo_combobox_yd")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### COMBOBOX

# COMMAND ----------

dbutils.widgets.get("vdemo_text_yd")

# COMMAND ----------

# MAGIC %md
# MAGIC ### REMOVE
# MAGIC 
# MAGIC Removes an input widget from the notebook
# MAGIC 
# MAGIC `remove(name: String): void `

# COMMAND ----------

dbutils.widgets.remove("demo_text_nd")

# COMMAND ----------

# MAGIC %md
# MAGIC ### REMOVE ALL
# MAGIC 
# MAGIC Removes all widgets in the notebook
# MAGIC 
# MAGIC `removeAll: void `

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCALA
