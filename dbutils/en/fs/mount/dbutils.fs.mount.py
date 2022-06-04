# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### TRABAJANDO CON FSUTILS.MOUNT
# MAGIC 
# MAGIC permite proporcionar acceso local a los archivos almacenados en la nube.
# MAGIC 
# MAGIC 1.    'dbutils.fs'
# MAGIC 2.   '%fs'
# MAGIC 3.   '%sh'
# MAGIC 
# MAGIC 
# MAGIC dato el caso que quisieramos trabajar con ARN esta seria el modo
# MAGIC ```scala
# MAGIC dbutils.fs.mount("s3a://<s3-bucket-name>", "/mnt/<s3-bucket-name>",
# MAGIC   extraConfigs = Map(
# MAGIC     "fs.s3a.credentialsType" -> "AssumeRole",
# MAGIC     "fs.s3a.stsAssumeRole.arn" -> "arn:aws:iam::<bucket-owner-acct-id>:role/MyRoleB",
# MAGIC     "spark.hadoop.fs.s3a.acl.default" -> "BucketOwnerFullControl"
# MAGIC   )
# MAGIC )
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 	source="wasbs://<container-name>@<storage-account-name>.blob.core.windows.net"
# MAGIC 	mount_point="/mnt/<mount_name>",
# MAGIC 	extra_config= {
# MAGIC 	"fs.azure.accout.key.<storage-account-name>.blob.core.windows.net": "<AccountKey>"

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
