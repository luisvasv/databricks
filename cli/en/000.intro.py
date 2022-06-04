# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC for work with cli is necesary to have configured a databricks tocken and installed databricks-cli
# MAGIC 
# MAGIC 
# MAGIC please follow the next steps to start:

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC pleasee, install some tool for  Python’s virtual environments
# MAGIC 
# MAGIC 
# MAGIC could by conda
# MAGIC 
# MAGIC or virtual env
# MAGIC 
# MAGIC then create environment and execute:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC `pip install databricks-cli` or `pip install databricks-cli --upgrade` in case that you nedd and upgrade

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC to ensure that the installation was suscessfull, pleas execute `databricks -h `
# MAGIC 
# MAGIC you can se someting like that:
# MAGIC 
# MAGIC ```bash
# MAGIC Usage: databricks [OPTIONS] COMMAND [ARGS]...
# MAGIC 
# MAGIC Options:
# MAGIC   -v, --version   0.16.6
# MAGIC   --debug         Debug Mode. Shows full stack trace on error.
# MAGIC   --profile TEXT  CLI connection profile to use. The default profile is
# MAGIC                   "DEFAULT".
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
# MAGIC   repos             Utility to interact with Repos.
# MAGIC   runs              Utility to interact with the jobs runs.
# MAGIC   secrets           Utility to interact with Databricks secret API.
# MAGIC   stack             [Beta] Utility to deploy and download Databricks resource
# MAGIC                     stacks.
# MAGIC   tokens            Utility to interact with Databricks tokens.
# MAGIC   workspace         Utility to interact with the Databricks workspace.
# MAGIC   
# MAGIC   ```
# MAGIC   
# MAGIC and `databricks --version`
# MAGIC   
# MAGIC `Version 0.16.6`
# MAGIC 
# MAGIC note: the version will change.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC we’ll need to set up authentication for it. To do this, we use a Databricks personal access token. 
# MAGIC To generate a token,go to settings  >> usert settings:
# MAGIC 
# MAGIC ```
# MAGIC - generate new token
# MAGIC   set up 
# MAGIC     - comment 
# MAGIC     - liftime
# MAGIC     - generate
# MAGIC     - copy token (once closed the pop up your cannot see again the token)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC now that we have token, we can set up authentication to use the Databricks CLI. To do this, open a command prompt and type in the following command:
# MAGIC 
# MAGIC `databricks configure --token`
# MAGIC 
# MAGIC you can seee something like that
# MAGIC 
# MAGIC ```bash
# MAGIC Databricks Host (should begin with https://): add workspace host
# MAGIC Token:  add token provided by databricks
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC if everythin is fine, you'll see the list of the user folder allowed in your workspace
# MAGIC 
# MAGIC `databricks workspace ls /Users/`
