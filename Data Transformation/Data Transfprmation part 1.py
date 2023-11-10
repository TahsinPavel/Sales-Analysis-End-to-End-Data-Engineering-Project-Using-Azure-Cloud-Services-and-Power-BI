# Databricks notebook source
dbutils.fs.ls("/mnt/salesdatadeproject/SalesLT/")


# COMMAND ----------

dbutils.fs.ls("/mnt/transformeddata1/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Transform All Tables
# MAGIC

# COMMAND ----------

tables = []

for i in dbutils.fs.ls("/mnt/salesdatadeproject/SalesLT/"):
    tables.append(i.name.split('/')[0])

# COMMAND ----------

tables

# COMMAND ----------

#Libraries

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

for i in tables:
    path = '/mnt/salesdatadeproject/SalesLT/' + i + '/' + i + '.parquet'
    data = spark.read.format("parquet").load(path)
    column = data.columns
    for col in column:
        if "Date" in col or "date" in col:
            data = data.withColumn(col, date_format(from_utc_timestamp(data[col].cast(TimestampType()),"UTC"), "yyyy-MM-dd"))

    output_path = '/mnt/transformeddata1/SalesLT/' + i + '/'
    data.write.format('delta').mode("overwrite").save(output_path)

# COMMAND ----------

display(data)
