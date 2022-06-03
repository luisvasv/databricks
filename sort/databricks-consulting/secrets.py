# Databricks notebook source
# MAGIC %md
# MAGIC ```python
# MAGIC pip install databricks-cli
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC ```bash
# MAGIC 
# MAGIC ▓▒░ databricks -h                                                                                                                                                                                                     ░▒▓ ✔  base Py 
# MAGIC Usage: databricks [OPTIONS] COMMAND [ARGS]...
# MAGIC 
# MAGIC Options:
# MAGIC   -v, --version   0.14.3
# MAGIC   --debug         Debug Mode. Shows full stack trace on error.
# MAGIC   --profile TEXT  CLI connection profile to use. The default profile is
# MAGIC                   "DEFAULT".
# MAGIC 
# MAGIC   -h, --help      Show this message and exit.
# MAGIC 
# MAGIC Commands:
# MAGIC   cluster-policies  Utility to interact with Databricks cluster policies.
# MAGIC   clusters          Utility to interact with Databricks clusters.
# MAGIC   configure         Configures host and authentication info for the CLI.
# MAGIC   fs                Utility to interact with DBFS.
# MAGIC   groups            Utility to interact with Databricks groups.
# MAGIC   instance-pools    Utility to interact with Databricks instance pools.
# MAGIC   jobs              Utility to interact with jobs.
# MAGIC   libraries         Utility to interact with libraries.
# MAGIC   pipelines         Utility to interact with the Databricks Delta Pipelines.
# MAGIC   runs              Utility to interact with the jobs runs.
# MAGIC   secrets           Utility to interact with Databricks secret API.
# MAGIC   stack             [Beta] Utility to deploy and download Databricks resource
# MAGIC                     stacks.
# MAGIC 
# MAGIC   tokens            Utility to interact with Databricks tokens.
# MAGIC   workspace         Utility to interact with the Databricks workspace.
# MAGIC   
# MAGIC ```

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# listando secretos
dbutils.secrets.listScopes()

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs secrets help

# COMMAND ----------


