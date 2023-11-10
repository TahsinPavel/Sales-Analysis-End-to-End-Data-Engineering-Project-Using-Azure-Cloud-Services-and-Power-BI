# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://salesdatadeproject@salesdatadeproject.dfs.core.windows.net/",   # contrainer@storageacc
  mount_point = "/mnt/salesdatadeproject",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/salesdatadeproject/SalesLT/")

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://transformeddata1@salesdatadeproject.dfs.core.windows.net/",   # contrainer@storageacc
  mount_point = "/mnt/transformeddata1",
  extra_configs = configs)

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://transformeddata2@salesdatadeproject.dfs.core.windows.net/",   # contrainer@storageacc
  mount_point = "/mnt/transformeddata2",
  extra_configs = configs)

