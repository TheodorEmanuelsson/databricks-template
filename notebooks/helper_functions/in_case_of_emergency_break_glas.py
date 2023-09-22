# Databricks notebook source

import pandas as pd 
from datetime import date

# COMMAND ----------

PRISKLASS = ['SE1','SE2','SE3','SE4']
end_date = date.today().strftime("%Y-%m-%d")
DATUM = pd.date_range(start='2022-10-26', end=end_date).strftime("%Y-%m-%d") # Format: YYYY-MM-DD

for d in DATUM:
    for p in PRISKLASS:

        df_json = pd.read_json(f"./data/{p}/eldata_{d[:4]}-{d[5:7]}-{d[8:]}.json")
        
        print(len(df_json))
        #spark_df = spark.createDataFrame(df_json)
        #spark_df.write.mode("overwrite").format('json').save(f"abfss://landingdev@landing9wvb5m.dfs.core.windows.net/{p}/")