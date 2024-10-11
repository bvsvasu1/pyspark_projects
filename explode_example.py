# Databricks notebook source
data = [
    ("Alice", "badminton,Tennis"),
    ("Bob", "carroms,Football"),
    ("Juli", "Cricket,Tennis")
]

# Step 3: Define the schema (column names)
columns = ["Name", "Hobbies"]

# Step 4: Create the DataFrame
df = spark.createDataFrame(data, schema=columns)
