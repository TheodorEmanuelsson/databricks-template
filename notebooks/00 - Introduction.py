# Databricks notebook source
import pandas as pd
import requests
from datetime import date
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType, FloatType, DecimalType
from pyspark.sql.functions import sha1, col, initcap, to_timestamp

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Run a child notebook

# COMMAND ----------

# MAGIC %run ./helper_functions/fake_data

# COMMAND ----------

# MAGIC %md
# MAGIC Call ```get_fake_data()``` 

# COMMAND ----------

data = get_fake_data()

# COMMAND ----------

data

# COMMAND ----------

schema = StructType([ \
    StructField("id",IntegerType(),True), \
    StructField("first_name",StringType(),True), \
    StructField("last_name",StringType(),True), \
    StructField("email",StringType(),True), \
    StructField("gender",StringType(),True), \
    StructField("ip_address", StringType(), True)
  ])
type(schema)

# COMMAND ----------

spark_df = spark.createDataFrame(data=data, schema=schema)
display(spark_df)

# COMMAND ----------

spark_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transformera data

# COMMAND ----------

spark_df = spark_df \
            .withColumnRenamed("id", "user_id") \
            .withColumn("email", sha1(col("email"))) \
            .withColumn("first_name", initcap(col("first_name"))) \
            .withColumn("last_name", initcap(col("last_name")))

display(spark_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Spara data 

# COMMAND ----------

# MAGIC %md
# MAGIC Skriv data till direkt till en tabell
# MAGIC
# MAGIC ```df.write.option("").mode("").saveAsTable("<catalog>.<schema>.<table>")```

# COMMAND ----------

spark_df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("fake_data")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Läs in data som vi sparade i tidigare del 
# MAGIC
# MAGIC ```%sql select * from <catalog>.<schema>.<table>```
# MAGIC
# MAGIC eller 
# MAGIC
# MAGIC ```spark.read.table("<catalog>.<schema>.<table>")```

# COMMAND ----------

df = spark.table("fake_data")

# COMMAND ----------

# MAGIC %md
# MAGIC View data

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Count

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Describe data

# COMMAND ----------

df.describe().show()

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
@pandas_udf("float")
def multiply(s: pd.Series, t: pd.Series) -> pd.Series:
    return s * t

spark.udf.register("multiply", multiply)
spark.sql("SELECT SEK_per_kWh, EXR, multiply(SEK_per_kWh,EXR) FROM emanuel_db.bronze.elpris_bronze").show()
