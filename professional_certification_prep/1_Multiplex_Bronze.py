# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

/Workspace/Repos/bvsvasu1@gmail.com/pyspark_practice/professional_certification_prep/1_Multiplex_Bronze

# COMMAND ----------

# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %run /Workspace/Repos/bvsvasu1@gmail.com/pyspark_practice/professional_certification_prep/Includes/Copy-Datasets

# COMMAND ----------

 schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo_pro/checkpoints/bronze_test", recurse=True)

# COMMAND ----------

def process_bronze():
    read_schema = StructType([
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", LongType()),
        StructField("offset", LongType()),
        StructField("timestamp", LongType())
    ])
    query = (spark.readStream.format("cloudFiles")
             .option("cloudFiles.format", "json")
             .schema(read_schema)
             .load(f"{dataset_bookstore}/kafka-raw")
             .withColumn("timestamp",F.from_unixtime(F.col("timestamp")/1000))
             .withColumn("year_month", F.date_format(F.col("timestamp"), "yyyy-MM"))
             .writeStream
             .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/bronze_test")
             .option("mergeSchema",True)
             .partitionBy("topic", "year_month")
             .trigger(availableNow=True).table("bronze_test")
             )
    query.awaitTermination()

# COMMAND ----------

process_bronze()

# COMMAND ----------

batch_df = spark.table("bronze_test")
display(batch_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select decode(key, "UTF-8")
# MAGIC from default.bronze_test
