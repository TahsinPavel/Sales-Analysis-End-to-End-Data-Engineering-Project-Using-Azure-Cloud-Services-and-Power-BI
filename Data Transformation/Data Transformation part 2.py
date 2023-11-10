# Databricks notebook source
# MAGIC %md
# MAGIC ## Load Data from transformeddata1 container

# COMMAND ----------

dbutils.fs.ls('mnt/transformeddata1/SalesLT/')

# COMMAND ----------

dbutils.fs.ls('mnt/transformeddata2/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Transfrom all Tables from transfromdata1 container to transformdata2 container
# MAGIC

# COMMAND ----------

tables = []

for i in dbutils.fs.ls("/mnt/transformeddata1/SalesLT/"):
    tables.append(i.name.split('/')[0])

# COMMAND ----------

tables

# COMMAND ----------

for name in tables:
    path = '/mnt/transformeddata1/SalesLT/' + name 
    print(path)
    data = spark.read.format('delta').load(path)

    # Get list of the columns names
    column_names = data.columns

    for old_col in column_names:
        # convert column name from ColumnName to Column_Name
        new_col_name = "".join(["_" + char if char.isupper() and not old_col[i-1].isupper() else char for i, char in enumerate(old_col)]).lstrip("_")

        # Change Column name using withColumnRenamed and regexp_replace
        data = data.withColumnRenamed(old_col, new_col_name)

    output_path = '/mnt/transformeddata2/SalesLT/' + name + '/'
    data.write.format('delta').mode('overwrite').option("mergeSchema", "true").save(output_path)

# COMMAND ----------

display(data)
