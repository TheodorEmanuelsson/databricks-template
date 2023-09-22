# Databricks notebook source

import pandas as pd
import requests
from datetime import date

# COMMAND ----------
# MAGIC %md
# MAGIC Hämta Eldata via API
##### GET https://www.elprisetjustnu.se/api/v1/prices/[ÅR]/[MÅNAD]-[DAG]_[PRISKLASS].json
# COMMAND ---------- 
PRISKLASS = ['SE1','SE2','SE3','SE4']
end_date = date.today().strftime("%Y-%m-%d")
DATUM = pd.date_range(start='2022-10-26', end=end_date).strftime("%Y-%m-%d") # Format: YYYY-MM-DD

# COMMAND ---------- 
for d in DATUM:
    for p in PRISKLASS:
        api_url = f'https://www.elprisetjustnu.se/api/v1/prices/{d[:4]}/{d[5:7]}-{d[8:]}_{p}.json'
        print(api_url)

        response = requests.get(url=api_url)
        print(response.status_code)
        data_json = response.json()

        df = pd.DataFrame(data=data_json)

        #df.to_json(f"./data/{p}/eldata_{d[:4]}-{d[5:7]}-{d[8:]}.json")


        


