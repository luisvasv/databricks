# Databricks notebook source
# MAGIC %md
# MAGIC # WIDGETS
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://is2-ssl.mzstatic.com/image/thumb/Purple114/v4/f4/c5/51/f4c551c7-7c20-1103-9738-e761e4a89ade/source/200x200bb.jpg" alt="ConboBox" style="width: 100">
# MAGIC </div>

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
# MAGIC  

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
# MAGIC #### DEFAULT MULTI VALUE

# COMMAND ----------

# no supported

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
    defaultValue="python ..",
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

dbutils.widgets.get("demo_dropdown_yd")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### COMBOBOX

# COMMAND ----------

dbutils.widgets.get("demo_combobox_yd")

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
# MAGIC ### WIDGET FROM CODE
# MAGIC 
# MAGIC you can create elements from code, example:

# COMMAND ----------

dbutils.widgets.dropdown(name="code_python", defaultValue="option # 1", choices=[f"option # {x}" for x in range(1, 10)], label="from for")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## SCALA

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

# MAGIC %scala
# MAGIC dbutils.widgets.combobox(
# MAGIC   
# MAGIC   "demo_combobox_yd",
# MAGIC   "delta",
# MAGIC   Seq("databricks", "spark", "delta"),
# MAGIC   "combobox_default_value"
# MAGIC )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITHOUT DEFAULT VALUE

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.widgets.combobox(
# MAGIC   "demo_combobox_nd",
# MAGIC   "",
# MAGIC   Seq("databricks", "spark", "delta"),
# MAGIC   "combobox_no_value"
# MAGIC )

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

# MAGIC %scala
# MAGIC dbutils.widgets.dropdown(
# MAGIC   "demo_dropdown_yd",
# MAGIC   "delta",
# MAGIC   Seq("databricks", "spark", "delta"),
# MAGIC   "dropdown_default_value"
# MAGIC )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITHOUT DEFAULT VALUE

# COMMAND ----------

# MAGIC %md
# MAGIC ```bash
# MAGIC # date : 18/06/2022 
# MAGIC # no puede ser creado sin valores por defecto, si se crea se vera un error tal como:
# MAGIC 
# MAGIC DefaultValueNotInChoicesList: Selection sequence must include 
# MAGIC 
# MAGIC ```

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

# MAGIC %scala
# MAGIC dbutils.widgets.multiselect(
# MAGIC     "vdemo_multiselect_yd",
# MAGIC     "delta",
# MAGIC     Seq("databricks", "spark", "delta", "sparksql"),
# MAGIC     "multiselect_default_value"
# MAGIC )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITHOUT DEFAULT VALUE

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC 
# MAGIC ```
# MAGIC # date : 18/06/2022 
# MAGIC # no puede ser creado sin valores por defecto, si se crea se vera un error tal como:
# MAGIC 
# MAGIC com.databricks.dbutils_v1.DefaultValueNotInChoicesList: Selection sequence must include 
# MAGIC ```

# COMMAND ----------

# MAGIC %md 
# MAGIC #### DEFAULT MULTI VALUE

# COMMAND ----------

# MAGIC %scala
# MAGIC // no supported

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

# MAGIC %scala
# MAGIC dbutils.widgets.text(
# MAGIC     "vdemo_text_yd",
# MAGIC     "scala ..",
# MAGIC     "text_default_value"
# MAGIC     )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITHOUT DEFAULT VALUE

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.widgets.text(
# MAGIC     "demo_text_nd",
# MAGIC     "",
# MAGIC     "text_no_value"
# MAGIC     )

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

# MAGIC %scala
# MAGIC dbutils.widgets.get("vdemo_multiselect_yd")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### TEXT

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.widgets.get("vdemo_text_yd")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### DROPDOWN

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.widgets.get("demo_combobox_yd")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### COMBOBOX

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.widgets.get("demo_combobox_yd")

# COMMAND ----------

# MAGIC %md
# MAGIC ### REMOVE
# MAGIC 
# MAGIC Removes an input widget from the notebook
# MAGIC 
# MAGIC `remove(name: String): void `

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.widgets.remove("demo_text_nd")

# COMMAND ----------

# MAGIC %md
# MAGIC ### REMOVE ALL
# MAGIC 
# MAGIC Removes all widgets in the notebook
# MAGIC 
# MAGIC `removeAll: void `

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.widgets.removeAll

# COMMAND ----------

# MAGIC %md
# MAGIC ### WIDGET FROM CODE
# MAGIC 
# MAGIC you can create elements from code, example:

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.widgets.dropdown("code_scala", "option # 1", List.range(1, 10).map(x=> s"option # ${x}"), "from for")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS test_widgets(
# MAGIC id_state STRING,
# MAGIC description STRING,
# MAGIC active STRING
# MAGIC  ) COMMENT "table to validate widgets from sql";
# MAGIC  
# MAGIC INSERT INTO test_widgets VALUES (1, "demo 1", "S");
# MAGIC INSERT INTO test_widgets VALUES (2, "demo 2", "S");
# MAGIC INSERT INTO test_widgets VALUES (3, "demo 3", "N");
# MAGIC INSERT INTO test_widgets VALUES (4, "demo 4", "S");
# MAGIC INSERT INTO test_widgets VALUES (5, "demo 5", "N");

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM  test_widgets;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM  test_widgets;

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
# MAGIC `CREATE WIDGET COMBOBOX <name> DEFAULT <defult value> CHOICES <query> `

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITH DEFAULT VALUE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET COMBOBOX sql_combobox DEFAULT 'demo 1' CHOICES SELECT description FROM test_widgets;

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITHOUT DEFAULT VALUE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET COMBOBOX sql_combobox_nd DEFAULT '' CHOICES SELECT description FROM test_widgets;

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
# MAGIC `CREATE WIDGET DROPDOWN <name> DEFAULT <defult value> CHOICES <query> `

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITH DEFAULT VALUE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET DROPDOWN sql_dropdown_yd DEFAULT 'demo 1' CHOICES SELECT description FROM test_widgets;

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITHOUT DEFAULT VALUE

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC # date : 18/06/2022 
# MAGIC # no puede ser creado sin valores por defecto, si se crea se vera un error tal como:
# MAGIC # CREATE WIDGET DROPDOWN sql_dropdown_nd DEFAULT '' CHOICES SELECT description FROM test_widgets;
# MAGIC 
# MAGIC DefaultValueNotInChoicesList: Selection sequence must include 
# MAGIC 
# MAGIC ```

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
# MAGIC `CREATE WIDGET DROPDOWN <name> DEFAULT <defult value> CHOICES <query>`

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITH DEFAULT VALUE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET MULTISELECT sql_multiselect_yd DEFAULT 'demo 1' CHOICES SELECT description FROM test_widgets;

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITHOUT DEFAULT VALUE

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC # date : 18/06/2022 
# MAGIC # no puede ser creado sin valores por defecto, si se crea se vera un error tal como:
# MAGIC # CREATE WIDGET MULTISELECT sql_multiselect_nd DEFAULT '' CHOICES SELECT description FROM test_widgets;
# MAGIC 
# MAGIC DefaultValueNotInChoicesList: Selection sequence must include 

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
# MAGIC `CREATE WIDGET TEXT <name> [DEFAULT <value>]`

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITH DEFAULT VALUE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT sql_text_yd DEFAULT "sql.."; 

# COMMAND ----------

# MAGIC %md 
# MAGIC #### WITHOUT DEFAULT VALUE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT sql_text_nd DEFAULT ""; 

# COMMAND ----------

# MAGIC %md
# MAGIC ### GET
# MAGIC 
# MAGIC  get current value of an input widget
# MAGIC  

# COMMAND ----------

# MAGIC %md 
# MAGIC #### GETARGUMENT

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *, getArgument("sql_dropdown_yd") as widget
# MAGIC FROM test_widgets
# MAGIC WHERE description = getArgument("sql_dropdown_yd")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### '%$WIDGET NAME%'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM test_widgets
# MAGIC WHERE description LIKE '%$sql_dropdown_yd%';

# COMMAND ----------

# MAGIC %md 
# MAGIC #### MIX

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *, getArgument("sql_dropdown_yd") as widget
# MAGIC FROM test_widgets
# MAGIC WHERE description LIKE '%$sql_dropdown_yd%';

# COMMAND ----------

# MAGIC %md
# MAGIC ### REMOVE
# MAGIC 
# MAGIC Removes an input widget from the notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC REMOVE WIDGET sql_dropdown_yd;
