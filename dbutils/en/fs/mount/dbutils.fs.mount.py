# Databricks notebook source
# MAGIC %md
# MAGIC  
# MAGIC  ### TRABAJANDO CON FSUTILS.MOUNT
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

# ayuda 
dbutils.fs.help()

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
