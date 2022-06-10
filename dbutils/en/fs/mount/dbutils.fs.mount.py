# Databricks notebook source
# MAGIC %md 
# MAGIC # MOUNT USING DBUTILS
# MAGIC 
# MAGIC -----
# MAGIC  ```
# MAGIC mount
# MAGIC mount(source: String, mountPoint: String, encryptionType: String = "", owner: String = null, extraConfigs: Map = Map.empty[String, String]): boolean -> Mounts the given source directory into DBFS at the given mount point
# MAGIC mounts: Seq -> Displays information about what is mounted within DBFS
# MAGIC refreshMounts: boolean -> Forces all machines in this cluster to refresh their mount cache, ensuring they receive the most recent information
# MAGIC unmount(mountPoint: String): boolean -> Deletes a DBFS mount point
# MAGIC updateMount(source: String, mountPoint: String, encryptionType: String = "", owner: String = null, extraConfigs: Map = Map.empty[String, String]): boolean -> Similar to mount(), but updates an existing mount point (if present) instead of creating a new one
# MAGIC 
# MAGIC  ```
# MAGIC 
# MAGIC TO DO

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## AZURE

# COMMAND ----------

# MAGIC %md
# MAGIC  ### BASICO
# MAGIC  
# MAGIC  TRABAJANDO CON FSUTILS.MOUNT
# MAGIC  
# MAGIC  permite proporcionar acceso local a los archivos almacenados en la nube.
# MAGIC  
# MAGIC  1.    'dbutils.fs'
# MAGIC  2.   '%fs'
# MAGIC  3.   '%sh'
# MAGIC  
# MAGIC  
# MAGIC  dato el caso que quisieramos trabajar con ARN esta seria el modo
# MAGIC  ```scala
# MAGIC  dbutils.fs.mount("s3a://<s3-bucket-name>", "/mnt/<s3-bucket-name>",
# MAGIC    extraConfigs = Map(
# MAGIC      "fs.s3a.credentialsType" -> "AssumeRole",
# MAGIC      "fs.s3a.stsAssumeRole.arn" -> "arn:aws:iam::<bucket-owner-acct-id>:role/MyRoleB",
# MAGIC      "spark.hadoop.fs.s3a.acl.default" -> "BucketOwnerFullControl"
# MAGIC    )
# MAGIC  )
# MAGIC  
# MAGIC  ```
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  	source="wasbs://<container-name>@<storage-account-name>.blob.core.windows.net"
# MAGIC  	mount_point="/mnt/<mount_name>",
# MAGIC  	extra_config= {
# MAGIC  	"fs.azure.accout.key.<storage-account-name>.blob.core.windows.net": "<AccountKey>"
# MAGIC      
# MAGIC      (1) service principal - usuario
# MAGIC      https://docs.databricks.com/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access.html
# MAGIC      
# MAGIC     ``` 
# MAGIC     
# MAGIC   ```    
# MAGIC  https://docs.databricks.com/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access.html
# MAGIC  https://docs.databricks.com/_static/notebooks/adls-gen2-service-principal.html
# MAGIC 
# MAGIC  
# MAGIC  
# MAGIC  ```

# COMMAND ----------

# MAGIC %md 
# MAGIC ## MOUNT

# COMMAND ----------

# MAGIC %md
# MAGIC ### AZURE

# COMMAND ----------

# MAGIC %md
# MAGIC  #### AVANZADO
# MAGIC  
# MAGIC  TRABAJANDO CON FSUTILS.MOUNT
# MAGIC  
# MAGIC  permite proporcionar acceso local a los archivos almacenados en la nube.
# MAGIC  
# MAGIC  1.    'dbutils.fs'
# MAGIC  2.   '%fs'
# MAGIC  3.   '%sh'
# MAGIC  
# MAGIC 
# MAGIC  ```
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  	source="wasbs://<container-name>@<storage-account-name>.blob.core.windows.net"
# MAGIC  	mount_point="/mnt/<mount_name>",
# MAGIC  	extra_config= {
# MAGIC  	"fs.azure.accout.key.<storage-account-name>.blob.core.windows.net": "<AccountKey>"
# MAGIC      
# MAGIC      (1) service principal - usuario
# MAGIC      https://docs.databricks.com/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access.html
# MAGIC   
# MAGIC  https://docs.databricks.com/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access.html
# MAGIC  https://docs.databricks.com/_static/notebooks/adls-gen2-service-principal.html
# MAGIC 
# MAGIC  
# MAGIC  
# MAGIC  ```
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC  ```
# MAGIC  
# MAGIC  to execute the steps listed below is necesary to have and user and group.
# MAGIC  azure portarl
# MAGIC    (0) is a optional step, but for more security, is important that the all resources that
# MAGIC        will be created, are in a resource group for trasability, segmentations and billing.
# MAGIC        got to resources group:
# MAGIC          set up:
# MAGIC            - choose suscription
# MAGIC            - resource group name
# MAGIC            - region
# MAGIC            - review + create
# MAGIC        all step listed below, is widely recommended configure in resource group
# MAGIC  
# MAGIC        
# MAGIC    (1) go to » storage account » create storage account >> app registration:
# MAGIC      set up:
# MAGIC        - storage account name
# MAGIC        - region
# MAGIC        - redundancy (LRS - locally redundant storage)
# MAGIC        - set extra config that you need (in case of ti be necessary)
# MAGIC        - create
# MAGIC        
# MAGIC    (2) inside storage account created
# MAGIC      - create container
# MAGIC      
# MAGIC    (3) got to » azure active directory >> app registration:
# MAGIC      - Register an application 
# MAGIC          set up: 
# MAGIC            - name, 
# MAGIC            - Lab Directory Only - Single tenant
# MAGIC            - register
# MAGIC            
# MAGIC    (4) in app created copy the next values:
# MAGIC      - app name
# MAGIC      - application (client) id
# MAGIC      - directory (tenant) ID
# MAGIC      
# MAGIC    (5) inside azure active directory » certificates & secrets
# MAGIC       - client secrets >>  add new clint secretg
# MAGIC         set up
# MAGIC           - description
# MAGIC           - expires
# MAGIC           - add
# MAGIC       - copy the secret value
# MAGIC         
# MAGIC   (6) go to » acces control (IAM)  >> add
# MAGIC      set up
# MAGIC        - add lore assigment
# MAGIC        - role (storage blob data contributor)
# MAGIC        - select group
# MAGIC        - save
# MAGIC        
# MAGIC    (7) go to » key vault >> create key vault
# MAGIC       set up
# MAGIC         - subscription
# MAGIC         - resource group
# MAGIC         - key value name
# MAGIC         - region
# MAGIC         - priciing tier (standar)
# MAGIC         - review + create
# MAGIC         
# MAGIC     (8) inside key vaulut  >> secrets  >> generate/import
# MAGIC         set up
# MAGIC           - name
# MAGIC           - value
# MAGIC           - content type (optional, identyfier)
# MAGIC           - create
# MAGIC           
# MAGIC           
# MAGIC         all steps above must be replicated for the next values:
# MAGIC           - app name
# MAGIC           - secret value
# MAGIC           - application (client) id
# MAGIC           - directory (tenant) ID
# MAGIC      
# MAGIC      (9) inside key vaulut  >> properties  >> generate/import
# MAGIC          - copy valut uri
# MAGIC          - resource id
# MAGIC          
# MAGIC      (10) create secrests scope scope 
# MAGIC      
# MAGIC        - way # 1 : get defautl databrucls url when you click on (azure, databricks, workspace name, databricLaunch Workspace),
# MAGIC          then add the segment `secrets/createScope`
# MAGIC        
# MAGIC            base    : <url>secrets/createScope
# MAGIC            example : https://adb-xxxxx.azuredatabricks.net/xxx#secrets/createScope
# MAGIC            
# MAGIC          set up
# MAGIC            - scope name
# MAGIC            - manage principal (creator)
# MAGIC            - dns name (vault uri)
# MAGIC            - resource id 
# MAGIC            - create
# MAGIC            
# MAGIC            for more information see  [https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes]
# MAGIC            
# MAGIC        - way # 2 : usin databricks cli
# MAGIC        
# MAGIC            for more references please see:
# MAGIC              repo main directory  >> cli >> secrets
# MAGIC        
# MAGIC        
# MAGIC  
# MAGIC  ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###### PYTHON

# COMMAND ----------

dbutils.secrets.list(scope="kvluchito")

# COMMAND ----------

       - app name
          - secret value
          - application (client) id
          - directory (tenant) ID
        
        creado
        storage
            datalakegen2lucho
        containers
            BRONZE
            silver
            gold
        

# COMMAND ----------



# COMMAND ----------

account_name = "datalakegen2lucho"
container_name = "bronze"
mount_point = "/mnt/bronze"

application_id = dbutils.secrets.get(scope="kvluchito", key="SPDATABRICKS-APPID")
tenant_id = dbutils.secrets.get(scope="kvluchito",key="SPDATABRICKS-TENID")
autentication_key = dbutils.secrets.get(scope="kvluchito",key="SPDATABRICKS-SV")
source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/" # + folder specific
endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

config = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": autentication_key,
    "fs.azure.account.oauth2.client.endpoint": endpoint
}

endpoint

# COMMAND ----------

dbutils.fs.mount(
  source=source,
  mount_point=mount_point,
  extra_configs=config)

# COMMAND ----------

# checking mounted sources
for mount in dbutils.fs.mounts():
    print(f"mounted : {mount}")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs ls /mnt/bronze

# COMMAND ----------

# MAGIC %sh cat /dbfs/mnt/bronze/bronze.log

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SCALA

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val account_name = "datalakegen2lucho"
# MAGIC val container_name = "silver"
# MAGIC val mount_point = "/mnt/silver"
# MAGIC 
# MAGIC val application_id = dbutils.secrets.get(scope="kvluchito", key="SPDATABRICKS-APPID")
# MAGIC val tenant_id = dbutils.secrets.get(scope="kvluchito",key="SPDATABRICKS-TENID")
# MAGIC val autentication_key = dbutils.secrets.get(scope="kvluchito",key="SPDATABRICKS-SV")
# MAGIC val source = s"abfss://$container_name@$account_name.dfs.core.windows.net/" // + folder specific
# MAGIC val endpoint = s"https://login.microsoftonline.com/$tenant_id/oauth2/token"
# MAGIC 
# MAGIC 
# MAGIC val configs = Map(
# MAGIC   "fs.azure.account.auth.type" -> "OAuth",
# MAGIC   "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC   "fs.azure.account.oauth2.client.id" -> application_id,
# MAGIC   "fs.azure.account.oauth2.client.secret" -> autentication_key,
# MAGIC   "fs.azure.account.oauth2.client.endpoint" -> endpoint
# MAGIC )
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = source,
# MAGIC   mountPoint = "/mnt/silver",
# MAGIC   extraConfigs = configs)

# COMMAND ----------

# MAGIC %scala
# MAGIC // checking mounted sources
# MAGIC dbutils.fs.mounts.foreach(mount => println(s"mounted : ${mount.mountPoint} | source :  ${mount.source} : encryptionType : ${mount.encryptionType}"))  

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.mounts)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SPARK

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account-name>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account-name>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account-name>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account-name>.dfs.core.windows.net", dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account-name>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## UNMOUNT

# COMMAND ----------

# MAGIC %md 
# MAGIC ### PYTHON

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.unmount("/mnt/bronze")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SCALA

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.unmount("/mnt/silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MOUNTS

# COMMAND ----------

# MAGIC %md 
# MAGIC ### PYTHON

# COMMAND ----------

# checking mounted sources
for mount in dbutils.fs.mounts():
    print(f"mounted : {mount}")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SCALA

# COMMAND ----------

# MAGIC %scala
# MAGIC // checking mounted sources
# MAGIC dbutils.fs.mounts.foreach(mount => println(s"mounted : ${mount.mountPoint} | source :  ${mount.source} : encryptionType : ${mount.encryptionType}"))  

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dbutils.fs.mounts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AWS

# COMMAND ----------

# MAGIC %md
# MAGIC ### BASICO

# COMMAND ----------

# MAGIC %scala
# MAGIC //definimos las variables para acceder a la cuenta de AWS, lo idea es que estubieran por secrets
# MAGIC val awsAccessKey = "xxxx"
# MAGIC val awsSecretKey = "xxx%xx"
# MAGIC val awsUrl = s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common"
# MAGIC val awsAuth = s"${awsAccessKey}:${awsSecretKey}"
# MAGIC val awsGenericPath = "s3a://databricks-corp-training/common"
# MAGIC dbutils.fs.mount(awsUrl, "/mnt/demo/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### AVANZADO

# COMMAND ----------

# ayuda 
dbutils.fs.help()

# COMMAND ----------

 
 dato el caso que quisieramos trabajar con ARN esta seria el modo
 ```scala
 dbutils.fs.mount("s3a://<s3-bucket-name>", "/mnt/<s3-bucket-name>",
   extraConfigs = Map(
     "fs.s3a.credentialsType" -> "AssumeRole",
     "fs.s3a.stsAssumeRole.arn" -> "arn:aws:iam::<bucket-owner-acct-id>:role/MyRoleB",
     "spark.hadoop.fs.s3a.acl.default" -> "BucketOwnerFullControl"
   )
 )
 

# COMMAND ----------

# podemos comprobar si efectivamente pudo montar los datos donde se requerian
%fs ls /mnt/

# COMMAND ----------

# se muestran los datos externos motontados
dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %scala
# MAGIC //validaciones basicas con scala
# MAGIC dbutils.fs.mounts.foreach(data => println(data.mountPoint, data.source))

# COMMAND ----------

# MAGIC %scala
# MAGIC //generando mensajes de validacion
# MAGIC val awsGenericPath = "s3a://databricks-corp-training/common"
# MAGIC val data = dbutils.fs.mounts.filter(data => data.source == awsGenericPath).head
# MAGIC println(s"punto de entrada : ${data.mountPoint}, y fueron montados desde : ${data.source}")

# COMMAND ----------

# MAGIC %scala
# MAGIC //Obliga a todas las máquinas de este clúster a actualizar su caché de montaje, lo que garantiza que reciban la información más reciente.
# MAGIC dbutils.fs.refreshMounts

# COMMAND ----------

#permite desmontar una unidad
dbutils.fs.unmount("/mnt/demo/")

# COMMAND ----------

# MAGIC %fs ls /mnt/
