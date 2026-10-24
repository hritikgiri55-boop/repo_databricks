# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark = SparkSession.builder.appName("test").getOrCreate()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import functions as F

df = spark.read.format("csv")\
                        .option("inferSchema", True)\
                        .option("header", True)\
                        .load("/Volumes/super_store_catalog/super_store_schema/super_store_volume/Currated_file/")

df.display()

# COMMAND ----------

hash_value = df.withColumn(
                "Hash_value",
                    F.sha2(
                        F.to_json(
                            F.struct(
                                  "Row_id",
                                  "Order_id",
                                  "Segment",
                                  "Country"
                        )       
                            ),
                        256                
                    )
)
hash_value.display()

# COMMAND ----------

filter = hash_value.filter(col("hash_value") =="634482e60796b7003457b5b5bb15b0c52187496c7f71e897bd02ecde589af5f0")
filter.display()
