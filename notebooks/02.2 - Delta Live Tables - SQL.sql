-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-1.png"/>
-- MAGIC </div>
-- MAGIC   
-- MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
-- MAGIC
-- MAGIC Let's use it to our pipeline and ingest the raw JSON & CSV data being delivered in our blob cloud storage. 

-- COMMAND ----------

-- DBTITLE 1,Ingest raw elpris data in incremental mode
CREATE STREAMING LIVE TABLE elpriser_bronze (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Raw data from source (el)"
AS SELECT * FROM cloud_files("/databricks_academy/raw/elpriser/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ Enforce quality and materialize our tables for Data Analysts
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-2.png"/>
-- MAGIC </div>
-- MAGIC
-- MAGIC The next layer often call silver is consuming **incremental** data from the bronze one, and cleaning up some information.
-- MAGIC
-- MAGIC We're also adding an [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html) on different field to enforce and track our Data Quality. This will ensure that our dashboard are relevant and easily spot potential errors due to data anomaly.
-- MAGIC
-- MAGIC These tables are clean and ready to be used by the BI team!

-- COMMAND ----------

-- DBTITLE 1,Clean el-data
CREATE STREAMING LIVE TABLE elpriser (
  CONSTRAINT SEK_per_kWh_valid EXPECT (SEK_per_kWh IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES (pipelines.autoOptimize.zOrderCols = "time_start")
COMMENT "User data cleaned for analysis."
AS SELECT
  SEK_per_kWh,
  EUR_per_kWh,
  EXR as exchange_rate,
  to_date(time_start) as date_start,
  to_timestamp(time_start) as time_start,
  to_timestamp(time_end) as time_end,
  elzon
from STREAM(live.elpriser_bronze)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/ Aggregate and join data to create our ML features
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-3.png"/>
-- MAGIC </div>
-- MAGIC
-- MAGIC We're now ready to create the features required for our Churn prediction.
-- MAGIC
-- MAGIC We need to enrich our user dataset with extra information which our model will use to help predicting churn, sucj as:
-- MAGIC
-- MAGIC * last command date
-- MAGIC * number of item bought
-- MAGIC * number of actions in our website
-- MAGIC * device used (ios/iphone)
-- MAGIC * ...

-- COMMAND ----------

-- DBTITLE 1,Create the feature table

CREATE LIVE TABLE view_elpriser
COMMENT "Final table"
AS 
SELECT
  date_start,
  SEK_per_kWh,
  EUR_per_kWh,
  exchange_rate,
  elzon,
  time_start,
  time_end
  FROM live.elpriser
