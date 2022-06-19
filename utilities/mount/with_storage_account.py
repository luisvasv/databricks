# Databricks notebook source
def mounted_zone(zone:str) -> bool:
    data = [mount.mountPoint for mount in dbutils.fs.mounts() if mount.mountPoint == zone]
    return False if len(data) == 0 else True

# COMMAND ----------

def mount_validation(zone:str) -> bool:
    status = True
    if mounted_zone(zone):
        print(f"[WARNING] the zone {zone} is mounted, no actions was made")
    else:
        try:
            print(f"[INFO] the zone {zone} isn't mounted, actions will be made")
            account_name = "datalakegen2lucho"
            container_name = zone
            mount_point = f"/mnt/{zone}"

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
            dbutils.fs.mount(
              source=source,
              mount_point=mount_point,
              extra_configs=config
            )
            print(f"[INFO] the zone {zone} was mounted, process is OK!")
        except Exception as ex:
            status = False
            print(f"[ERROR] errors where found, please check. \n + {ex}")
                
    return status
        

# COMMAND ----------

if mount_validation("/mnt/" + dbutils.widgets.get("zone")):
    print(f"[INFO] the process has finished without errors")
else:
    print(f"[ERROR] the process has finished with errors")

# COMMAND ----------

# dbutils.widgets.text(
#    name="zone", 
#    defaultValue="", 
#    label="zone"
#)
