# Databricks notebook source
import pandas as pd
from pandas.tseries.offsets import MonthEnd

# COMMAND ----------

def create_calendar(database, start='2000-01-01', end='2099-12-31'):
    df = pd.DataFrame({"Date": pd.date_range(start, end)})
    df["Day_Name"] = df.Date.dt.day_name()
    df["Day"] = df.Date.dt.day
    df["Week"] = df.Date.dt.isocalendar().week
    df["Month_Name"] = df.Date.dt.month_name()
    df["Month"] = df.Date.dt.month
    df["Quarter"] = df.Date.dt.quarter
    df["Year"] = df.Date.dt.year
    df["Year_half"] = (df.Quarter + 1) // 2
    df['FY'] = df.Date.dt.to_period('Q-AUG').dt.qyear
    df['EndOfMonth'] = pd.to_datetime(df['Date'], format="%Y%m") + MonthEnd(0)
    
    spark.createDataFrame(df).write.mode("overwrite").saveAsTable(f"{database}.bronze.calendar_bronze")
    return -1


# COMMAND ----------


