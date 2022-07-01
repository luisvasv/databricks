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
            mount_point = f"/mnt/{zone}"
            if dbutils.fs.mkdirs(mount_point):
                print(f"[INFO] the zone {zone} was mounted, process is OK!")
            else:
                print(f"[WARNING] the zone {zone} coudn't be created, please check!")
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
