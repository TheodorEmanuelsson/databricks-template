# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Första delen

# COMMAND ----------

import pandas as pd
import requests
from datetime import date
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType, FloatType, DecimalType

# COMMAND ----------

# MAGIC %md
# MAGIC spark session

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC elprisetjustnu.se har ett öppet API för att hämta el-data från de senaste månaderna.
# MAGIC
# MAGIC Hämta eldata via API
# MAGIC GET https://www.elprisetjustnu.se/api/v1/prices/[ÅR]/[MÅNAD]-[DAG]_[PRISKLASS].json

# COMMAND ----------

# MAGIC %md
# MAGIC <table class="table table-sm mb-5">
# MAGIC             <tbody><tr>
# MAGIC               <th>Variabel</th>
# MAGIC               <th>Beskrivning</th>
# MAGIC               <th>Exempel</th>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <th>ÅR</th>
# MAGIC               <td>Alla fyra siffror</td>
# MAGIC               <td>2023</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <th>MÅNAD</th>
# MAGIC               <td>Alltid två siffror, med en inledande nolla</td>
# MAGIC               <td>03</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <th>DAG</th>
# MAGIC               <td>Alltid två siffror, med en inledande nolla</td>
# MAGIC               <td>13</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <th>PRISKLASS</th>
# MAGIC               <td>
# MAGIC                 SE1 = Luleå / Norra Sverige<br>
# MAGIC                 SE2 = Sundsvall / Norra Mellansverige<br>
# MAGIC                 SE3 = Stockholm / Södra Mellansverige<br>
# MAGIC                 SE4 = Malmö / Södra Sverige
# MAGIC               </td>
# MAGIC               <td>SE3</td>
# MAGIC             </tr>
# MAGIC           </tbody></table>

# COMMAND ----------

# MAGIC %md
# MAGIC Använd endpointen för att hämta historisk data. Tidigast tillängliga datum är 2022-10-26. 
# MAGIC
# MAGIC Lämpliga paket i Python att använda:
# MAGIC
# MAGIC - ```requests``` - API-anrop. [docs](https://requests.readthedocs.io/en/latest/)
# MAGIC - ```pandas``` - Pandas DataFrame. [docs](https://pandas.pydata.org/docs/)
# MAGIC - ```datetime``` - Datumhantering [docs](https://docs.python.org/3/library/datetime.html)
# MAGIC - ```pyspark``` - Datahantering i spark. [docs](https://spark.apache.org/docs/3.1.3/api/python/index.html)
# MAGIC
# MAGIC Skapa en funktion som loopar över en array med datum

# COMMAND ----------

# MAGIC %run ./helper_functions/calendar

# COMMAND ----------
elpriserRawDataDirectory = 'databricks_academy/raw/elpriser/'

def cleanup_folder(path):
  for f in dbutils.fs.ls(path):
    if f.name.startswith('_committed') or f.name.startswith('_started') or f.name.startswith('_SUCCESS') :
      dbutils.fs.rm(f.path)

# Ändra här
name = <name>
database = <database_name>
create_calendar(database=database)

# COMMAND ----------

PRISKLASS = ['SE1','SE2','SE3','SE4']
end_date = ...
start_date = ...
DATUM = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d") # Format: YYYY-MM-DD
DATUM

# COMMAND ----------

# MAGIC %md
# MAGIC skapa en funktion som loopar över datumen-arrayen definerad ovan

# COMMAND ----------

def fetch_data():

    data = []
    for p in PRISKLASS:
        for d in DATUM:
            api_url = f'https://www.elprisetjustnu.se/api/v1/prices/{d[:4]}/{d[5:7]}-{d[8:]}_{p}.json'
            print(api_url)
            response = requests.get(url=api_url)
            print(response.status_code)
            data_json = response.json()
            data_json = [dict(item, **{'elzon': p}) for item in data_json]
            data += data_json

    elpris = spark.createDataFrame(sc.parallelize(data))
    elpris = elpris.cache()
    elpris.repartition(10).write.format("json").mode("overwrite").option("overwriteSchema", True).save(elpriserRawDataDirectory)
    cleanup_folder(elpriserRawDataDirectory)

    return data    


# COMMAND ----------

data = fetch_data()

# COMMAND ----------

# Lista filer mappen
for fileInfo in dbutils.fs.ls(elpriserRawDataDirectory): print(fileInfo.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Skapa catalog (Traditionellt - Database)
# MAGIC ```%sql create catalog if not exists emanuel_db```

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists <database>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Skapa databas (Traditionellt - Schema)
# MAGIC ```%sql create schema if not exists <catalog>.bronze```

# COMMAND ----------

# MAGIC %sql 
# MAGIC create schema if not exists <database>.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Med Autoloader
# MAGIC
# MAGIC Ingest data med Autoloader.
# MAGIC ```.readStream``` används för inkrementell data laddning (streaming) - Spark bestämmer vilken ny data som inte ännu har processats. 
# MAGIC https://docs.databricks.com/en/structured-streaming/delta-lake.html 
# COMMAND ----------

name = "_".join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user').split("@")[0].split(".")[0:2])
deltaTablesDirectory = '/Users/'+name+'/elpriser/'
dbutils.fs.mkdirs(deltaTablesDirectory)

schema = 'bronze'
table = 'elpriser_bronze'

def ingest_folder(folder, data_format, table):
  bronze_products = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", data_format)
                      .option("cloudFiles.inferColumnTypes", "true")
                      .option("cloudFiles.schemaLocation",
                              f"{deltaTablesDirectory}/schema/{table}") #Autoloader will automatically infer all the schema & evolution
                      .load(folder))
  return (bronze_products.writeStream
            .option("checkpointLocation",
                    f"{deltaTablesDirectory}/checkpoint/{table}") #exactly once delivery on Delta tables over restart/kill
            .option("overwriteSchema", "true") #merge any new column dynamically
            .trigger(once = True) #Remove for real time streaming
            .table(table)) #Table will be created if we haven't specified the schema first
  
ingest_folder('/'+elpriserRawDataDirectory, 'json',  f'{database}.{schema}.{table}').awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from <database>.bronze.elpris_bronze
