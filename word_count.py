# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# COMMAND ----------

# DBTITLE 1,Approach 1
text_df = spark.read.options(header=True).text("/FileStore/tables/wordcount.txt")
df_words = text_df.select(split(text_df["value"], " ").alias("words"))
df_final = df_words.select(explode("words").alias("word"))
word_counts = df_final.groupBy("word").count()
word_counts.display()

# COMMAND ----------

# text_df = spark.read.options(header=True).text("/FileStore/tables/wordcount.txt")
# df_words = text_df.select(split(text_df["value"], " ").alias("words"))
# df_words_list = df_words.agg(collect_list("words").alias("all_words"))
# df_words_exploded = df_words_list.select(explode("all_words").alias("words"))
# df_final = df_words_exploded.select(explode("words").alias("word"))
# word_counts = df_final.groupBy("word").count()

# COMMAND ----------

udf_schema = StructType([
    StructField("word", StringType(), True),
    StructField("value", IntegerType(), True)
])

def word_count(line):
    line_split = line.split(" ")
    word_dict = {}
    print("line is:  ",line_split)
    for i in line_split:
        if i in word_dict:
            word_dict[i] = word_dict[i] +1
        else:
            word_dict[i] = 1
    print("line is:  ",word_dict)
    return word_dict

spark.udf.register("word_count", word_count, MapType(StringType(), IntegerType()))
word_count_udf = udf(word_count, MapType(StringType(), IntegerType()))

# COMMAND ----------

text_df = spark.read.options(header=True).text("/FileStore/tables/wordcount.txt")
text_df.createOrReplaceGlobalTempView("text_df_vw")
text_df_udf = text_df.withColumn("line_word_count", word_count_udf(text_df.value))
text_df_udf.printSchema()
text_df_breakup = text_df_udf.select(
    explode(text_df_udf.line_word_count).alias("word", "count")
)

text_df_breakup.display()

# COMMAND ----------

# DBTITLE 1,Using SQL
# MAGIC %sql
# MAGIC select explode(word_count(value))
# MAGIC from global_temp.text_df_vw

# COMMAND ----------

def word_count_2(line):
    line_split = line.split(" ")
    word_dict = {}
    for word in line_split:
        word_dict[word] = word_dict.get(word, 0) + 1
    return json.dumps(word_dict)  # Return JSON string

# Register the UDF with the return type as StringType
word_count_udf_2 = udf(word_count_2, StringType())

# COMMAND ----------

json_schema = MapType(StringType(), IntegerType())
text_df3 = spark.read.text("/FileStore/tables/wordcount.txt")
text_df_udf3 = text_df3.withColumn("line_word_count", word_count_udf_2(text_df3.value))
text_df_parsed3 = text_df_udf3.withColumn(
    "line_word_count_map",
    from_json(col("line_word_count"), json_schema)
)
text_df_exploded3 = text_df_parsed3.select(
    col("value"),
    explode(col("line_word_count_map")).alias("word", "count")
)
text_df_exploded3.display()

# COMMAND ----------


