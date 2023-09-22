# Databricks notebook source
# MAGIC %md
# MAGIC ##Sista delen i vår "Pipeline" - Gold

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ##Här vill vi joina elpris och kalender

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Läs in våra tabeller från silver för att få en överblick

# COMMAND ----------

# MAGIC %md 
# MAGIC Läs in data som vi sparade i tidigare del 
# MAGIC
# MAGIC ```%sql select * from <catalog>.<schema>.<table>```
# MAGIC
# MAGIC eller 
# MAGIC
# MAGIC ```spark.read.table("<catalog>.<schema>.<table>")```

# COMMAND ----------

database = <database>
source_schema = 'silver'
source_table_name = 'elpriser'

# COMMAND ----------

spark_el = spark.read.table(f"{database}.{source_schema}.{source_table_name}")
display(spark_el)

# COMMAND ----------

spark_el.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ladda in Kalendern

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emanuel_db.silver.calendar

# COMMAND ----------

# MAGIC %md
# MAGIC **Notera** att resultatet från SQL-frågan sparas i variabeln ```_sqldf```när man exekvera en SQL fråga med ```%sql select ...```

# COMMAND ----------

spark_cal = _sqldf

# COMMAND ----------

# MAGIC %md
# MAGIC ###Skapa vårt nya schema **Gold**

# COMMAND ----------

spark.sql("create schema if not exists {database}.gold")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Skapa en vy över tabellerna

# COMMAND ----------

source_schema = 'silver'
source_table = 'elpriser'

target_schema = 'gold'
target_view = 'view_elpriser'

spark.sql(
  f"""
    CREATE OR REPLACE view {database}.{target_schema}.{target_view} AS
        SELECT
            date_start,
            SEK_per_kWh,
            EUR_per_kWh,
            exchange_rate,
            elzon,
            time_start,
            time_end,
            Date,
            Day_Name,
            Day,
            Week,
            Month_Name,
            Month,
            Quarter,
            Year,
            Year_half,
            FY,
            EndOfMonth
        FROM {database}.{source_schema}.{source_table} el
        left JOIN 
        (select *, to_date(date) as date_start_cal from emanuel_db.staging.calendar) cal
        on el.date_start = cal.date_start_cal
  """
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from <database>.gold.view_elpriser

# COMMAND ----------


