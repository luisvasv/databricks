# Databricks notebook source
# MAGIC %md
# MAGIC # SECRETS
# MAGIC 
# MAGIC first of all, to start to works with secrets that can sincronyze with azure, you must to have
# MAGIC 
# MAGIC - vault uri
# MAGIC - resource id
# MAGIC 
# MAGIC if your don't have this information, you can see:
# MAGIC 
# MAGIC https://docs.microsoft.com/es-mx/azure/databricks/security/secrets/secret-scopes
# MAGIC 
# MAGIC 
# MAGIC else, you can work with databricks managed scope that for workspace is limited to a maximum of 100 secret scopes.
# MAGIC 
# MAGIC doble ckkkkkkkkkkkkkkkkkk

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## GENERAL HELP
# MAGIC databricks secrets -h   
# MAGIC 
# MAGIC  ```
# MAGIC Usage: databricks secrets [OPTIONS] COMMAND [ARGS]...
# MAGIC 
# MAGIC   Utility to interact with secret API.
# MAGIC 
# MAGIC Options:
# MAGIC   -v, --version   0.16.6
# MAGIC   --debug         Debug Mode. Shows full stack trace on error.
# MAGIC   --profile TEXT  CLI connection profile to use. The default profile is
# MAGIC                   "DEFAULT".
# MAGIC   -h, --help      Show this message and exit.
# MAGIC 
# MAGIC Commands:
# MAGIC   create-scope  Creates a secret scope.
# MAGIC   delete        Deletes a secret.
# MAGIC   delete-acl    Deletes an access control rule for a principal.
# MAGIC   delete-scope  Deletes a secret scope.
# MAGIC   get-acl       Gets the details for an access control rule.
# MAGIC   list          Lists all the secrets in a scope.
# MAGIC   list-acls     Lists all access control rules for a given secret scope.
# MAGIC   list-scopes   Lists all secret scopes.
# MAGIC   put           Puts a secret in a scope. "write" is an alias for "put".
# MAGIC   put-acl       Creates or overwrites an access control rule for a principal
# MAGIC                 applied to a given secret scope. "write-acl" is an alias for
# MAGIC                 "put-acl".
# MAGIC   write         Puts a secret in a scope. "write" is an alias for "put".
# MAGIC   write-acl     Creates or overwrites an access control rule for a principal
# MAGIC                 applied to a given secret scope. "write-acl" is an alias for
# MAGIC                 "put-acl".
# MAGIC  ```
# MAGIC  
# MAGIC  
# MAGIC  the first step that you must do to work with secrets y create  an scope, check the helpt first:
# MAGIC  
# MAGIC  databricks secrets create-scope -h 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC `databricks secrets create-scope -h`
# MAGIC  ```
# MAGIC Usage: databricks secrets create-scope [OPTIONS]
# MAGIC 
# MAGIC   Creates a new secret scope with given name.
# MAGIC 
# MAGIC Options:
# MAGIC   --scope SCOPE                   The name of the secret scope.  [required]
# MAGIC   --initial-manage-principal TEXT
# MAGIC                                   The initial principal that can manage the
# MAGIC                                   created secret scope. If specified, the
# MAGIC                                   initial ACL with MANAGE permission applied
# MAGIC                                   to the scope is assigned to the supplied
# MAGIC                                   principal (user or group). Currently, the
# MAGIC                                   only supported principal for this option is
# MAGIC                                   the group "users", which contains all users
# MAGIC                                   in the workspace. If not specified, the
# MAGIC                                   initial ACL with MANAGE permission applied
# MAGIC                                   to the scope is assigned to the request
# MAGIC                                   issuer's user identity.
# MAGIC   --scope-backend-type [AZURE_KEYVAULT|DATABRICKS]
# MAGIC                                   The backend that will be used for this
# MAGIC                                   secret scope. Options are (case-sensitive):
# MAGIC                                   1) 'AZURE_KEYVAULT' and 2) 'DATABRICKS'
# MAGIC                                   (default option) Note: To create an Azure
# MAGIC                                   Keyvault, be sure to configure an AAD Token
# MAGIC                                   using 'databricks configure --aad-token'
# MAGIC   --resource-id TEXT              The resource ID associated with the azure
# MAGIC                                   keyvault to be used as the backend for the
# MAGIC                                   secret scope. NOTE: Only use with azure-
# MAGIC                                   keyvault as backend
# MAGIC   --dns-name TEXT                 The dns name associated with the azure
# MAGIC                                   keyvault to be used as the backed for the
# MAGIC                                   secret scope. NOTE: Only use with azure-
# MAGIC                                   keyvault as backend
# MAGIC   --debug                         Debug Mode. Shows full stack trace on error.
# MAGIC   --profile TEXT                  CLI connection profile to use. The default
# MAGIC                                   profile is "DEFAULT".
# MAGIC   -h, --help                      Show this message and exit.
# MAGIC  ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## HELP

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## CREATE SCOPE AZURE
# MAGIC `databricks secrets create-scope --scope <scope-name> --scope-backend-type AZURE_KEYVAULT --resource-id <azure-keyvault-resource-id> --dns-name <azure-keyvault-dns-name>`
# MAGIC   
# MAGIC 
# MAGIC   
# MAGIC  ```
# MAGIC   '{"error_code":"INVALID_PARAMETER_VALUE",
# MAGIC     "message":"Scope with Azure KeyVault must have userAADToken defined!"}'
# MAGIC  ```
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC this error is a problem that has databricjs craeting scopes with azure, you can se more information here:
# MAGIC 
# MAGIC https://stackoverflow.com/questions/71414233/create-azure-key-vault-backed-secret-scope-in-databricks-with-aad-token

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREATE SCOPE DATABRICKS
# MAGIC 
# MAGIC 
# MAGIC  ```
# MAGIC databricks secrets create-scope --scope <scope-name>
# MAGIC  ```
# MAGIC  
# MAGIC  example
# MAGIC  
# MAGIC   ```
# MAGIC  databricks secrets create-scope --scope databricks-cli
# MAGIC   ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## LIST SCOPE
# MAGIC 
# MAGIC  `databricks secrets list-scope`
# MAGIC 
# MAGIC  ```
# MAGIC 
# MAGIC Scope           Backend         KeyVault URL
# MAGIC --------------  --------------  ----------------------------------
# MAGIC databricks-cli  DATABRICKS      N/A
# MAGIC 
# MAGIC 
# MAGIC  ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## DELETE SCOPE
# MAGIC 
# MAGIC Creat other scope
# MAGIC   ```
# MAGIC  databricks secrets create-scope --scope databricks-
# MAGIC   ```
# MAGIC   ``` 
# MAGIC 
# MAGIC  databricks secrets list-scope`
# MAGIC  ```
# MAGIC 
# MAGIC  ```bash
# MAGIC Scope           Backend         KeyVault URL
# MAGIC --------------  --------------  ----------------------------------
# MAGIC databricks      DATABRICKS      N/A
# MAGIC databricks-cli  DATABRICKS      N/A
# MAGIC 
# MAGIC  ```
# MAGIC  
# MAGIC  ``` 
# MAGIC  databricks secrets delete-scope --scope databricks
# MAGIC  ``` 
# MAGIC  
# MAGIC  
# MAGIC al listar nuevamente los secretos solo debe estar el que previamente creamo
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC  ```
# MAGIC Scope           Backend         KeyVault URL
# MAGIC --------------  --------------  ----------------------------------
# MAGIC databricks-cli  DATABRICKS      N/A

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## CREATE SECRET
# MAGIC 
# MAGIC  ```bash
# MAGIC 
# MAGIC databricks secrets put -h                             2 err  databricks py                                                                                                          ok  databricks py 
# MAGIC Usage: databricks secrets put [OPTIONS]
# MAGIC 
# MAGIC   Puts a secret in the provided scope with the given name. Overwrites any
# MAGIC   existing value if the name exists.
# MAGIC 
# MAGIC   You should specify at most one option in "string-value" and "binary-file".
# MAGIC 
# MAGIC   If "string-value", the argument will be stored in UTF-8 (MB4) form.
# MAGIC 
# MAGIC   If "binary-file", the argument should be a path to file. File content will
# MAGIC   be read as secret value and stored as bytes.
# MAGIC 
# MAGIC   If none of "string-value" and "binary-file" specified, an editor will be
# MAGIC   opened for inputting secret value. The value will be stored in UTF-8 (MB4)
# MAGIC   form.
# MAGIC 
# MAGIC   "databricks secrets write" is an alias for "databricks secrets put", and
# MAGIC   will be deprecated in a future release.
# MAGIC 
# MAGIC Options:
# MAGIC   --scope SCOPE        The name of the secret scope.  [required]
# MAGIC   --key KEY            The name of the secret key.  [required]
# MAGIC   --string-value TEXT  Read value from string and stored in UTF-8 (MB4) form
# MAGIC   --binary-file PATH   Read value from binary-file and stored as bytes.
# MAGIC   --debug              Debug Mode. Shows full stack trace on error.
# MAGIC   --profile TEXT       CLI connection profile to use. The default profile is
# MAGIC                        "DEFAULT".
# MAGIC 
# MAGIC Usage: databricks secrets put [OPTIONS]
# MAGIC 
# MAGIC   Puts a secret in the provided scope with the given name. Overwrites any
# MAGIC   existing value if the name exists.
# MAGIC 
# MAGIC   You should specify at most one option in "string-value" and "binary-file".
# MAGIC 
# MAGIC   If "string-value", the argument will be stored in UTF-8 (MB4) form.
# MAGIC 
# MAGIC   If "binary-file", the argument should be a path to file. File content will
# MAGIC   be read as secret value and stored as bytes.
# MAGIC 
# MAGIC   If none of "string-value" and "binary-file" specified, an editor will be
# MAGIC   opened for inputting secret value. The value will be stored in UTF-8 (MB4)
# MAGIC   form.
# MAGIC 
# MAGIC   "databricks secrets write" is an alias for "databricks secrets put", and
# MAGIC   will be deprecated in a future release.
# MAGIC 
# MAGIC Options:
# MAGIC   --scope SCOPE        The name of the secret scope.  [required]
# MAGIC   --key KEY            The name of the secret key.  [required]
# MAGIC   --string-value TEXT  Read value from string and stored in UTF-8 (MB4) form
# MAGIC   --binary-file PATH   Read value from binary-file and stored as bytes.
# MAGIC   --debug              Debug Mode. Shows full stack trace on error.
# MAGIC   --profile TEXT       CLI connection profile to use. The default profile is
# MAGIC                        "DEFAULT".
# MAGIC   -h, --help           Show this message and exit.
# MAGIC  ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### CREATE SECRET FROM STRING VALUE
# MAGIC 
# MAGIC 
# MAGIC ```bash
# MAGIC  databricks secrets [put|write] --scope <scope-name> --key <secret-name> --string-value <secret-value>
# MAGIC  
# MAGIC  # example 
# MAGIC  databricks secrets put --scope databricks-cli --key url_databricks --string-value "https://databricks.com/"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### (TO DO)CREATE FROM BINARY FILE

# COMMAND ----------

# MAGIC %md
# MAGIC ### (TO DO)CREATE FROM TEXT FILE USING PROFILE

# COMMAND ----------

# MAGIC %md
# MAGIC ### (TO DO)CREATE WITH DEBUG

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## LIST SECRET
# MAGIC 
# MAGIC 
# MAGIC ```bash
# MAGIC databricks secrets list -h                                                                                                                                                       2 err  databricks py 
# MAGIC Usage: databricks secrets list [OPTIONS]
# MAGIC 
# MAGIC   Lists the secret keys that are stored at this scope. Also lists the last
# MAGIC   updated timestamp (UNIX time in milliseconds) if available.
# MAGIC 
# MAGIC Options:
# MAGIC   --scope SCOPE    The name of the secret scope.  [required]
# MAGIC   --output FORMAT  can be "JSON" or "TABLE". Set to TABLE by default.
# MAGIC   --debug          Debug Mode. Shows full stack trace on error.
# MAGIC   --profile TEXT   CLI connection profile to use. The default profile is
# MAGIC                    "DEFAULT".
# MAGIC   -h, --help       Show this message and exit.
# MAGIC 
# MAGIC ```

# COMMAND ----------

### LIST SECRETS JSON


```bash
databricks secrets -list --scope <scope-name>

```


```bash

databricks secrets list --scope databricks-cli

Key name          Last updated
--------------  --------------
url_databricks   1654807720495


```

# COMMAND ----------

### LIST SECRETS DEBUG


```
databricks secrets -list --scope <scope-name> --debug
```


```bash

databricks secrets list --scope databricks-cli --debug                                                                                                                              ok  databricks py 
HTTP debugging enabled
send: b'GET /api/2.0/secrets/list?scope=databricks-cli HTTP/1.1\r\nHost: adb-<your-host>.16.azuredatabricks.net\r\nuser-agent: databricks-cli-0.16.6-secrets-list-<secret-id> \r\nAccept-Encoding: gzip, deflate\r\nAccept: */*\r\nConnection: keep-alive\r\nAuthorization: Bearer <other-id>-2\r\nContent-Type: text/json\r\n\r\n'
reply: 'HTTP/1.1 200 OK\r\n'
header: date: Thu, 9 Jun 2022 21:03:15 GMT
header: strict-transport-security: max-age=31536000; includeSubDomains; preload
header: x-content-type-options: nosniff
header: content-type: application/json
header: x-databricks-org-id: <your-host>
header: content-encoding: gzip
header: vary: Accept-Encoding
header: server: databricks
header: transfer-encoding: chunked
Key name          Last updated
--------------  --------------
url_databricks   1654807720495


```

# COMMAND ----------

### LIST SECRETS PROFILE


```bash


```

# COMMAND ----------

# MAGIC %md
# MAGIC ## DELETE SECRET
# MAGIC 
# MAGIC 
# MAGIC ```bash
# MAGIC databricks secrets delete -h                                                                                                                                                        ok  databricks py 
# MAGIC Usage: databricks secrets delete [OPTIONS]
# MAGIC 
# MAGIC   Deletes the secret stored in this scope.
# MAGIC 
# MAGIC Options:
# MAGIC   --scope SCOPE   The name of the secret scope.  [required]
# MAGIC   --key KEY       The name of the secret key.  [required]
# MAGIC   --debug         Debug Mode. Shows full stack trace on error.
# MAGIC   --profile TEXT  CLI connection profile to use. The default profile is
# MAGIC                   "DEFAULT".
# MAGIC 
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC basic sintaxis
# MAGIC 
# MAGIC `databricks secrets delete --scope <scope-name> --key <secret-name>`
# MAGIC 
# MAGIC 
# MAGIC step
# MAGIC 
# MAGIC ```
# MAGIC databricks secrets put --scope databricks-cli --key url_databricks_2 --string-value "https://google.com/" 
# MAGIC ```
# MAGIC 
# MAGIC ```
# MAGIC databricks secrets list --scope databricks-cli                                                                                                                                     
# MAGIC Key name            Last updated
# MAGIC ----------------  --------------
# MAGIC url_databricks     1654807720495
# MAGIC url_databricks_2   1654809188798
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC databricks secrets delete --scope databricks-cli --key url_databricks_2                                                                                                             ok  databricks py 
# MAGIC  ~  databricks secrets list --scope databricks-cli                                                                                                                                      ok  databricks py 
# MAGIC Key name          Last updated
# MAGIC --------------  --------------
# MAGIC url_databricks   1654807720495
# MAGIC 
# MAGIC ```
