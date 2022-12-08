# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting ADLS using Secrets

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list("formula1-scope")

# COMMAND ----------

dbutils.secrets.get(scope = "formula1-scope", key="client-id")

# COMMAND ----------

storage_account_name = "f1dlcourse"
client_id = dbutils.secrets.get(scope = "formula1-scope", key="client-id")
tenant_id = dbutils.secrets.get(scope = "formula1-scope", key="tenant-id")
client_secret = dbutils.secrets.get(scope = "formula1-scope", key="client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

#dbutils.fs.unmount("/mnt/f1dlcourse/raw")
#dbutils.fs.unmount("/mnt/f1dlcourse/processed")

# COMMAND ----------

def mountadls(container_name):
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)


# COMMAND ----------

mountadls("presentation")

# COMMAND ----------

mountadls("raw")
mountadls("processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/f1dlcourse/presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/f1dlcourse/processed")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------


