# Databricks notebook source
# MAGIC %md
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

# COMMAND ----------

# MAGIC %md
# MAGIC # wit data
# MAGIC databricks secrets create-scope -h 
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
# MAGIC 
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

`databricks --scope training 

 ```
databricks secrets create-scope --scope <scope-name>
