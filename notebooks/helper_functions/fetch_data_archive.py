# Databricks notebook source
import pandas as pd
import requests
from datetime import date

# COMMAND ----------

# MAGIC %md
# MAGIC Hämta Eldata via API
# MAGIC ##### GET https://www.elprisetjustnu.se/api/v1/prices/[ÅR]/[MÅNAD]-[DAG]_[PRISKLASS].json

# COMMAND ----------

PRISKLASS = ['SE1','SE2','SE3','SE4']
end_date = date.today().strftime("%Y-%m-%d")
DATUM = pd.date_range(start='2022-10-26', end=end_date).strftime("%Y-%m-%d") # Format: YYYY-MM-DD

# COMMAND ----------
# TODO fixa så den appendar json eller dfs och skriver 
name = 'emanuel'
for p in PRISKLASS:
    data_json = []
    for d in DATUM:
        api_url = f'https://www.elprisetjustnu.se/api/v1/prices/{d[:4]}/{d[5:7]}-{d[8:]}_{p}.json'
        print(api_url)

        response = requests.get(url=api_url)
        print(response.status_code)
        data_json = response.json()

    df = pd.DataFrame(data=data_json)
        
    #df.to_json(f"./data/{p}/eldata_{d[:4]}-{d[5:7]}-{d[8:]}.json")
        
    spark_df = spark.createDataFrame(df)
    print(f"abfss://landing@landing123emhol.dfs.core.windows.net/{name}/{p}/")
    spark_df.write.mode("overwrite").format('json').save(f"abfss://landing@landing123emhol.dfs.core.windows.net/{name}/{p}/") #{path}/{current_date}/")



# COMMAND ----------

print("hej")

# COMMAND ----------


# COMMAND ----------


