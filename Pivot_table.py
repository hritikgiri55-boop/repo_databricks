# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Creating_pivot").getOrCreate()

row = [
    ("101", "IT", "34000"),
    ("101", "IT", "23422"),
    ("101", "IT", "43222"),
    ("101", "HR", "12500"),
    ("202", "HR", "54222"),
    ("202", "HR", "76888"),
    ("404", "Civil", "31444"),
    ("404", "Civil", "78566"),
    ("505", "Sales", "58999"),
    ("505", "Sales", "23933"),
]

column = ["Cust_id", "Dept", "Salary"]

df = spark.createDataFrame(row, column)

pivot = (
    df.groupBy("Cust_id")
    .pivot("Dept", ["IT", "HR", "Civil", "Sales"])
    .agg(count(col("Salary")))
)


pivot.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create table catalog_dirty_table.default.sales_pivot(
# MAGIC -- Name string,
# MAGIC -- Id long,
# MAGIC -- Dept string
# MAGIC -- )
# MAGIC -- using delta;
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   catalog_dirty_table.default.sales_pivot

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("Pivot_table").getOrCreate()
data = [
    (1, "Pen", "Jan", 100),
    (2, "Pen", "Feb", 120),
    (3, "Pen", "Mar", 130),
    (4, "Pen", "Apr", 140),
    (5, "Pencil", "Jan", 200),
    (6, "Pencil", "Feb", 220),
    (7, "Pencil", "Mar", 210),
    (8, "Pencil", "Apr", 230),
    (9, "Eraser", "Jan", 50),
    (10, "Eraser", "Feb", 60),
    (11, "Eraser", "Mar", 55),
    (12, "Eraser", "Apr", 65),
    (13, "Scale", "Jan", 80),
    (14, "Scale", "Feb", 90),
    (15, "Scale", "Mar", 95),
]

columns = ["Order_id", "Product", "Month", "Amount"]

df_1 = spark.createDataFrame(data, columns)

# display(df_1)

df_1.printSchema()

# COMMAND ----------

Pivot = (
    df_1.groupBy("Product").pivot("Month", ["Jan", "Feb", "Mar", "Apr"]).sum("Amount")
)
Pivot.display()

# COMMAND ----------

df_1.display()
df_1.explain(True)

# COMMAND ----------

df_1.createTempView("sample_table")

# COMMAND ----------

Temp_table = spark.sql("""
          
          select * from sample_table;
          
          """)
Temp_table.display()

# COMMAND ----------

pivot_df_1 = df_1.groupBy(col("Product")).pivot("Month").agg(sum("Amount"))
pivot_df_1.display()

# COMMAND ----------

df_1_loading = (
    df_1.write.format("csv")
    .mode("overwrite")
    .option("header", True)
    .save("/Volumes/sales_catalog/sales_schema/sales_volume/pivot_file.csv")
)

# COMMAND ----------

pivot_df_1.coalesce(1).write.format("csv").mode("overwrite").option(
    "header", True
).save(
    "/Volumes/super_store_catalog/super_store_schema/super_store_volume/pivot_file.csv"
)

# COMMAND ----------

pivot_df_1.explain(True)

# COMMAND ----------

pivot_df_1.write.format("delta").option("header", True).saveAsTable(
    "super_store_catalog.super_store_schema.pivot_table"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE
# MAGIC   super_store_catalog.super_store_schema.pivot_table
# MAGIC zorder by product;

# COMMAND ----------

Delta_read = spark.table("super_store_catalog.super_store_schema.pivot_table")
Delta_read.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended super_store_catalog.super_store_schema.pivot_table;
