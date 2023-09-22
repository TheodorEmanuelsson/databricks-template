# Databricks notebook source

#!pip install prophet==1.1.2 numpy==1.21
# COMMAND ----------
import pandas as pd
import numpy as np 
from matplotlib import pyplot as plt
from prophet import Prophet

# COMMAND ----------

df_train = pd.read_csv("./sales_data/train.csv")
df_train.date = pd.to_datetime(df_train['date'])
df_train.head()

# COMMAND ----------

plt.plot(df_train.date, df_train.sales)
# COMMAND ----------


df_train.plot('date' , 'sales')

# COMMAND ----------

df_train['year'] = df_train['date'].dt.year
df_train['month'] = df_train['date'].dt.month
df_train['day_of_week'] = df_train['date'].dt.day_of_week
df_train['dayofweek'] = df_train['date'].dt.dayofweek
df_train.head()


plt.plot(df_train.year.unique(), df_train.groupby('year').sum('sales'))
# COMMAND ----------


# instantiate the model and set parameters
model = Prophet(
 interval_width=0.95,
 growth='linear',
 daily_seasonality=False,
 weekly_seasonality=True,
 yearly_seasonality=True,
 seasonality_mode='multiplicative'
)

# COMMAND ----------


df_history = df_train[["date", "sales"]]
df_history.rename(columns={"date":'ds', "sales":'y'}, inplace=True)
df_history

# COMMAND ----------

model.fit(df_history)
# COMMAND ----------


future_pd = model.make_future_dataframe(
 periods=90,
 freq='d',
 include_history=True
)
future_pd

# COMMAND ----------

# predict over the dataset
forecast_pd = model.predict(future_pd)
forecast_pd
# COMMAND ----------


predict_fig = model.plot(forecast_pd, xlabel='date', ylabel='sales')





