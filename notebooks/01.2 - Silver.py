# Databricks notebook source
# MAGIC %md
# MAGIC ### Del 2 - Silver

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import sha1, col, initcap, to_timestamp, to_date

# COMMAND ----------

# MAGIC %md
# MAGIC ###Skapa schemat Silver
# MAGIC
# MAGIC Använd ```spark.sql("<Query>")``` för att skapa schemat i din catalog

# COMMAND ----------

database = <database
spark.sql(f"create schema if not exists {database}.silver")

# COMMAND ----------

# MAGIC %md 
# MAGIC Skriv data till ditt nya schema

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gör dessa transfomrationer med hjälp av ```spark.readStream```

# COMMAND ----------

# MAGIC %md
# MAGIC ###Berika kalendern

# COMMAND ----------

name = "_".join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user').split("@")[0].split(".")[0:2])
deltaTablesDirectory = '/Users/'+name+'/elpriser/'

source_schema = 'bronze'
source_table = 'calendar_bronze'

target_schema = 'silver'
target_table = 'calendar'

(spark.readStream
        .table(f'{database}.{source_schema}.{source_table}')
        .withColumn("date_start", to_date(col("date")))
      .writeStream
        .option("checkpointLocation", f"{deltaTablesDirectory}/checkpoint/calendar")
        .trigger(once=True)
        .table(f"{database}.{target_schema}.{target_table}").awaitTermination())

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Berika el-data

# COMMAND ----------

source_schema = 'bronze'
source_table = 'elpriser_bronze'

target_schema = 'silver'
target_table = 'elpriser'

(spark.readStream
        .table(f'{database}.{source_schema}.{source_table}')
        .withColumnRenamed("EXR", "exchange_rate")
        .withColumn("date_start", to_date(col("time_start")))
        .withColumn("time_end", to_timestamp(col("time_end")))
        .withColumn("time_start", to_timestamp(col("time_start")))
        .drop(col("_rescued_data"))
      .writeStream
        .option("checkpointLocation", f"{deltaTablesDirectory}/checkpoint/elpriser")
        .trigger(once=True)
        .table(f"{database}.{target_schema}.{target_table}").awaitTermination())

# COMMAND ----------
# MAGIC %md Checkpoint location
# COMMAND ----------
for fileInfo in dbutils.fs.ls(deltaTablesDirectory+'/checkpoint/elpriser'): print(fileInfo.name)

# COMMAND ----------

spark.read.table(f"{database}.{schema}.{table_name}").show()
