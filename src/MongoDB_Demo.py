# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # PUSL3121 - MongoDB/NoSQL Demonstration
# MAGIC ## Storing E-Commerce Event Data in MongoDB Atlas

# COMMAND ----------

# MAGIC %pip install "pymongo[srv]"

# COMMAND ----------

# Attempt Python restart after %pip when supported.
# In Serverless + Databricks Connect sessions, restartPython may be unavailable.
try:
    from databricks.sdk.runtime import dbutils

    dbutils.library.restartPython()
except Exception as e:
    print("Skipping restartPython in this environment.")
    print(f"Reason: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC 1. If Python restarted, re-run this notebook from this cell downward.
# MAGIC 2. Provide your MongoDB password in the `MONGO_PASSWORD` widget before connecting.

# COMMAND ----------

import json
from urllib.parse import quote_plus

from databricks.sdk.runtime import dbutils
from pymongo import MongoClient
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

try:
    spark
except NameError:
    try:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
    except Exception:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

DATA_PATH = "/Volumes/workspace/default/cosmetics_data"
MONTH_FILE = "2019-Oct"
TARGET_DOCS = 5000

dbutils.widgets.text("MONGO_USERNAME", "admin", "MongoDB Username")
dbutils.widgets.text("MONGO_PASSWORD", "", "MongoDB Password")
dbutils.widgets.text("MONGO_CLUSTER_HOST", "pusl3121-cosmetics.74d9hbu.mongodb.net", "MongoDB Cluster Host")

mongo_username = dbutils.widgets.get("MONGO_USERNAME")
mongo_password = dbutils.widgets.get("MONGO_PASSWORD")
mongo_cluster_host = dbutils.widgets.get("MONGO_CLUSTER_HOST")

if not mongo_password:
    raise ValueError("Please set MONGO_PASSWORD widget value, then re-run this cell.")

# COMMAND ----------

df = spark.read.csv(f"{DATA_PATH}/{MONTH_FILE}.csv", header=True, inferSchema=True)

if isinstance(df.schema["event_time"].dataType, StringType):
    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.regexp_replace(F.col("event_time"), " UTC", ""), "yyyy-MM-dd HH:mm:ss"),
    )

print(f"Loaded {df.count()} rows from {MONTH_FILE}.csv for MongoDB demo")

# COMMAND ----------

connection_string = (
    f"mongodb+srv://{quote_plus(mongo_username)}:{quote_plus(mongo_password)}"
    f"@{mongo_cluster_host}/?appName=PUSL3121-Cosmetics"
)

client = MongoClient(connection_string, serverSelectionTimeoutMS=15000)
client.admin.command("ping")

db = client["cosmetics_ecommerce"]
collection = db["events"]

print("Connected successfully!")
print("Database:", db.name)
print("Collection:", collection.name)

# COMMAND ----------

sample_df = df.filter(F.col("event_type") == "purchase").limit(TARGET_DOCS)
sample_count = sample_df.count()

if sample_count == 0:
    raise ValueError("No purchase events found in sample source file.")

rows = sample_df.collect()
sample_records = []

for row in rows:
    doc = row.asDict(recursive=True)
    for key, value in doc.items():
        # Normalize NaN values and numpy scalar-like objects before BSON insertion.
        if isinstance(value, float) and value != value:
            value = None
        elif hasattr(value, "item"):
            value = value.item()

        if key == "event_time" and value is not None:
            value = str(value)

        doc[key] = value
    sample_records.append(doc)

print(f"Prepared {len(sample_records)} documents for insertion")

# COMMAND ----------

collection.delete_many({})
result = collection.insert_many(sample_records, ordered=False)

inserted_count = len(result.inserted_ids)
mongo_count = collection.count_documents({})

print(f"Inserted {inserted_count} documents")
print(f"Total documents in collection: {mongo_count}")

# COMMAND ----------

sample_doc = collection.find_one()
print("Sample inserted document:")
print(json.dumps(sample_doc, indent=2, default=str))

brand_name = "runail"
brand_purchases = list(collection.find({"brand": brand_name}).limit(5))

print(f"\nSample purchases for brand '{brand_name}' ({len(brand_purchases)} shown):")
for doc in brand_purchases:
    print(json.dumps(doc, indent=2, default=str))

# COMMAND ----------

pipeline = [
    {"$match": {"brand": {"$nin": [None, ""]}, "price": {"$ne": None}}},
    {"$group": {"_id": "$brand", "avg_price": {"$avg": "$price"}, "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10},
]

results = list(collection.aggregate(pipeline))

print("Top 10 brands by purchase count (sample):")
for r in results:
    print(f"Brand: {r['_id']}, Avg Price: ${r['avg_price']:.2f}, Count: {r['count']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### NoSQL Concepts Demonstrated
# MAGIC - **Document model:** Each event is stored as a JSON-like MongoDB document.
# MAGIC - **Flexible schema:** Optional fields like `brand` and `category_code` can be null without schema migration.
# MAGIC - **Operational analytics:** Fast filtering and aggregation are shown with `find()` and `aggregate()`.
# MAGIC - **Scalable storage pattern:** Sampling (5,000 docs) is used to fit Atlas free-tier limits while proving end-to-end integration.
