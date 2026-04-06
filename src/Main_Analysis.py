# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # PUSL3121 - Big Data Analytics Coursework
# MAGIC ## Industry-Based Big Data Analytics Using Apache Spark
# MAGIC ### E-Commerce Cosmetics Shop — Customer Behavior Analysis
# MAGIC **Dataset:** eCommerce Events History in Cosmetics Shop (Kaggle)
# MAGIC **Industry:** E-Commerce / Retail
# MAGIC **Group Members:** [Name1], [Name2], [Name3], [Name4]
# MAGIC **Module:** PUSL3121 Big Data Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table of Contents
# MAGIC 1. Dataset Loading & Exploration (Step 1)
# MAGIC 2. Data Cleaning & Processing (Step 3)
# MAGIC 3. Data Visualization (Step 4)
# MAGIC 4. Predictive Analytics — Machine Learning (Step 5)
# MAGIC 5. Big Data Architecture Design (Step 6)
# MAGIC
# MAGIC **Note:** MongoDB/NoSQL demonstration is in the companion notebook: PUSL3121_MongoDB_Demo

# COMMAND ----------

# Configuration - set this once based on your upload method
from databricks.sdk.runtime import dbutils, display
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

try:
    spark
except NameError:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

DATA_PATH = "/Volumes/workspace/default/cosmetics_data"
files = dbutils.fs.ls(DATA_PATH)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dataset Loading & Exploration (Step 1)
# MAGIC Load all monthly files from DBR Volumes, validate schema, parse timestamps, and inspect data quality.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Industry Context and Business Problem (P1-T2)
# MAGIC This notebook focuses on executable analysis cells.
# MAGIC
# MAGIC Detailed narrative and finalized findings are documented in:
# MAGIC `docs/phase1_dataset_exploration_findings.md`

# COMMAND ----------


def refresh_spark_if_needed():
    global spark
    try:
        spark.sql("SELECT 1").collect()
        return False
    except Exception:
        try:
            from databricks.connect import DatabricksSession

            spark = DatabricksSession.builder.getOrCreate()
        except Exception:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
        print("Spark session refreshed.")
        return True


def load_monthly_dataset(data_path, month_list, log_month_counts=True):
    dfs = []
    for m in month_list:
        try:
            temp = spark.read.csv(f"{data_path}/{m}.csv", header=True, inferSchema=True)
            dfs.append(temp)
            if log_month_counts:
                print(f"Loaded {m}: {temp.count()} rows")
        except Exception as e:
            print(f"Could not load {m}: {e}")

    if not dfs:
        raise ValueError(f"No datasets could be loaded from {data_path}")

    # Serverless compute does not support DataFrame persist/cache.
    return reduce(lambda a, b: a.unionByName(b), dfs)


months = ["2019-Oct", "2019-Nov", "2019-Dec", "2020-Jan", "2020-Feb"]
refresh_spark_if_needed()
df = load_monthly_dataset(DATA_PATH, months, log_month_counts=True)
total_rows = df.count()
print(f"\nTotal rows: {total_rows}")

# COMMAND ----------

# CRITICAL: parse '2019-10-01 00:00:00 UTC' safely if it remains a string.
session_refreshed = refresh_spark_if_needed()
if session_refreshed or "df" not in globals():
    print("Re-loading DataFrame in the active Spark session...")
    df = load_monthly_dataset(DATA_PATH, months, log_month_counts=False)

df.printSchema()

if isinstance(df.schema["event_time"].dataType, StringType):
    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.regexp_replace(F.col("event_time"), " UTC", ""), "yyyy-MM-dd HH:mm:ss"),
    )
    print("Converted event_time from string to timestamp")

df.select("event_time").show(5, truncate=False)

# COMMAND ----------

df.printSchema()
print(f"Total rows (recheck): {df.count()}")
df.show(10, truncate=False)
df.describe().show(truncate=False)
df.select("event_type").distinct().show(truncate=False)

# COMMAND ----------

event_type_counts = df.groupBy("event_type").count().orderBy(F.col("count").desc())
event_type_counts.show(truncate=False)

# COMMAND ----------

null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 Findings Summary
# MAGIC - Data loaded and validated from the configured Volumes path.
# MAGIC - Schema, event distribution, and null profiling cells executed in this section.
# MAGIC - Final reported metrics and interpretation are maintained in:
# MAGIC   `docs/phase1_dataset_exploration_findings.md`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Cleaning & Processing (Step 3)
# MAGIC ### P3-T1: Data Cleaning
# MAGIC
# MAGIC This section performs full cleaning and preparation for downstream analytics and ML.
# MAGIC
# MAGIC Cleaning rationale:
# MAGIC - `category_code` and `brand` are filled with `unknown` to preserve high-volume behavioral rows for pattern analysis.
# MAGIC - Invalid or unusable records are dropped for core metrics integrity:
# MAGIC   - `price <= 0` or `price IS NULL`
# MAGIC   - `event_type IS NULL`
# MAGIC   - `event_time IS NULL` (needed for time-based features)

# COMMAND ----------

# P3-T1 continuity check: reuse existing df from Phase 1 when available.
# This block is intentionally self-healing so Section 2 can run in isolation
# inside VS Code Interactive sessions after kernel or Spark Connect resets.
if "F" not in globals():
    from pyspark.sql import functions as F

if "StringType" not in globals():
    from pyspark.sql.types import StringType

if "reduce" not in globals():
    from functools import reduce

if "spark" not in globals():
    try:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
    except Exception:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

if "refresh_spark_if_needed" not in globals():

    def refresh_spark_if_needed():
        global spark
        try:
            spark.sql("SELECT 1").collect()
            return False
        except Exception:
            try:
                from databricks.connect import DatabricksSession

                spark = DatabricksSession.builder.getOrCreate()
            except Exception:
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
            print("Spark session refreshed.")
            return True


if "load_monthly_dataset" not in globals():

    def load_monthly_dataset(data_path, month_list, log_month_counts=True):
        dfs = []
        for m in month_list:
            try:
                temp = spark.read.csv(f"{data_path}/{m}.csv", header=True, inferSchema=True)
                dfs.append(temp)
                if log_month_counts:
                    print(f"Loaded {m}: {temp.count()} rows")
            except Exception as e:
                print(f"Could not load {m}: {e}")

        if not dfs:
            raise ValueError(f"No datasets could be loaded from {data_path}")

        return reduce(lambda a, b: a.unionByName(b), dfs)


if "DATA_PATH" not in globals():
    DATA_PATH = "/Volumes/workspace/default/cosmetics_data"

if "months" not in globals():
    months = ["2019-Oct", "2019-Nov", "2019-Dec", "2020-Jan", "2020-Feb"]

session_refreshed = refresh_spark_if_needed()
if session_refreshed:
    print("Spark session was refreshed for this run.")

if "df" not in globals():
    print("DataFrame not found in memory - reloading monthly files...")
    df = load_monthly_dataset(DATA_PATH, months, log_month_counts=True)
else:
    print("DataFrame is available from Phase 1.")

if isinstance(df.schema["event_time"].dataType, StringType):
    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.regexp_replace(F.col("event_time"), " UTC", ""), "yyyy-MM-dd HH:mm:ss"),
    )
    print("Converted event_time from string to timestamp")

rows_before = df.count()
print(f"Rows before cleaning: {rows_before:,}")

# COMMAND ----------

# Null profile before cleaning.
null_counts_before = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
print("Null counts before cleaning:")
null_counts_before.show(truncate=False)

# COMMAND ----------

# Core cleaning strategy aligned to project findings.
df_clean = df.fillna({"category_code": "unknown", "brand": "unknown"})

df_clean = df_clean.filter(F.col("event_type").isNotNull())
df_clean = df_clean.filter(F.col("event_time").isNotNull())
df_clean = df_clean.filter(F.col("price").isNotNull() & (F.col("price") > 0))

# Optional strict deduplication for event-level records.
APPLY_DEDUPLICATION = False
if APPLY_DEDUPLICATION:
    df_clean = df_clean.dropDuplicates(["event_time", "user_id", "product_id", "event_type"])

# Feature engineering for temporal and category analysis.
df_clean = (
    df_clean.withColumn("event_date", F.to_date("event_time"))
    .withColumn("event_hour", F.hour("event_time"))
    .withColumn("event_dayofweek", F.dayofweek("event_time"))
    .withColumn("event_month", F.month("event_time"))
    .withColumn(
        "main_category",
        F.when(F.col("category_code") != "unknown", F.split(F.col("category_code"), "\\.").getItem(0)).otherwise(
            "unknown"
        ),
    )
)

cache_enabled = True
try:
    df_clean = df_clean.cache()
    rows_after = df_clean.count()
except Exception as e:
    cache_enabled = False
    print(f"Cache not available in this runtime. Continuing without cache. Reason: {e}")
    rows_after = df_clean.count()

rows_removed = rows_before - rows_after
rows_removed_pct = (rows_removed / rows_before) * 100 if rows_before else 0.0
rows_retained_pct = (rows_after / rows_before) * 100 if rows_before else 0.0

print(f"Rows after cleaning: {rows_after:,}")
print(f"Rows removed: {rows_removed:,} ({rows_removed_pct:.2f}%)")
print(f"Rows retained: {rows_after:,} ({rows_retained_pct:.2f}%)")
print(f"Cache status: {'enabled' if cache_enabled else 'not enabled'}")

# COMMAND ----------

# Null profile after cleaning.
null_counts_after = df_clean.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_clean.columns])
print("Null counts after cleaning:")
null_counts_after.show(truncate=False)

# Critical-column validation.
critical_cols = ["event_time", "event_type", "price", "category_code", "brand"]
critical_nulls_after = df_clean.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in critical_cols])
print("Critical column nulls after cleaning (should be 0):")
critical_nulls_after.show(truncate=False)

required_derived_cols = ["event_date", "event_hour", "event_dayofweek", "main_category"]
missing_derived_cols = [c for c in required_derived_cols if c not in df_clean.columns]
print(f"Derived column check: {'OK' if not missing_derived_cols else f'Missing -> {missing_derived_cols}'}")

# COMMAND ----------

df_clean.printSchema()

df_clean.select(
    "event_time",
    "event_date",
    "event_hour",
    "event_dayofweek",
    "event_month",
    "category_code",
    "main_category",
    "brand",
    "price",
    "event_type",
).show(10, truncate=False)

df_clean.createOrReplaceTempView("events_clean")
print("Temp view created: events_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ### P3-T2: Descriptive Analytics — Aggregations & Patterns
# MAGIC This section implements 8 required analyses for Step 3 with business-focused interpretation notes.
# MAGIC
# MAGIC Runtime note:
# MAGIC - In VS Code Interactive + Spark Connect workflows, session inactivity may clear in-memory objects.
# MAGIC - The preflight cell below rebuilds `df_clean` if needed so this section can run independently.

# COMMAND ----------

# P3-T2 preflight: make this section runnable even after session resets.
if "F" not in globals():
    from pyspark.sql import functions as F

if "StringType" not in globals():
    from pyspark.sql.types import StringType

if "reduce" not in globals():
    from functools import reduce

if "spark" not in globals():
    try:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
    except Exception:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

if "refresh_spark_if_needed" not in globals():

    def refresh_spark_if_needed():
        global spark
        try:
            spark.sql("SELECT 1").collect()
            return False
        except Exception:
            try:
                from databricks.connect import DatabricksSession

                spark = DatabricksSession.builder.getOrCreate()
            except Exception:
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
            print("Spark session refreshed.")
            return True


if "load_monthly_dataset" not in globals():

    def load_monthly_dataset(data_path, month_list, log_month_counts=True):
        dfs = []
        for m in month_list:
            try:
                temp = spark.read.csv(f"{data_path}/{m}.csv", header=True, inferSchema=True)
                dfs.append(temp)
                if log_month_counts:
                    print(f"Loaded {m}: {temp.count()} rows")
            except Exception as e:
                print(f"Could not load {m}: {e}")

        if not dfs:
            raise ValueError(f"No datasets could be loaded from {data_path}")

        return reduce(lambda a, b: a.unionByName(b), dfs)


if "DATA_PATH" not in globals():
    DATA_PATH = "/Volumes/workspace/default/cosmetics_data"

if "months" not in globals():
    months = ["2019-Oct", "2019-Nov", "2019-Dec", "2020-Jan", "2020-Feb"]

session_refreshed = refresh_spark_if_needed()
if session_refreshed:
    print("Spark session was refreshed for P3-T2.")

if "df_clean" not in globals():
    print("df_clean not found - rebuilding from source data...")

    if "df" not in globals():
        df = load_monthly_dataset(DATA_PATH, months, log_month_counts=True)

    if isinstance(df.schema["event_time"].dataType, StringType):
        df = df.withColumn(
            "event_time",
            F.to_timestamp(F.regexp_replace(F.col("event_time"), " UTC", ""), "yyyy-MM-dd HH:mm:ss"),
        )

    df_clean = df.fillna({"category_code": "unknown", "brand": "unknown"})
    df_clean = df_clean.filter(F.col("event_type").isNotNull())
    df_clean = df_clean.filter(F.col("event_time").isNotNull())
    df_clean = df_clean.filter(F.col("price").isNotNull() & (F.col("price") > 0))

    df_clean = (
        df_clean.withColumn("event_date", F.to_date("event_time"))
        .withColumn("event_hour", F.hour("event_time"))
        .withColumn("event_dayofweek", F.dayofweek("event_time"))
        .withColumn("event_month", F.month("event_time"))
        .withColumn(
            "main_category",
            F.when(F.col("category_code") != "unknown", F.split(F.col("category_code"), "\\.").getItem(0)).otherwise(
                "unknown"
            ),
        )
    )
else:
    print("df_clean found in memory.")

# Backward-compatibility fix for runs that used the old split pattern.
if "main_category" in df_clean.columns:
    legacy_main_category = (
        df_clean.filter(
            (F.col("main_category").isNotNull())
            & (F.col("main_category") != "unknown")
            & F.col("main_category").contains(".")
        )
        .limit(1)
        .count()
    )
    if legacy_main_category > 0:
        print("Detected legacy main_category values. Recomputing from category_code...")
        df_clean = df_clean.withColumn(
            "main_category",
            F.when(F.col("category_code") != "unknown", F.split(F.col("category_code"), "\\.").getItem(0)).otherwise(
                "unknown"
            ),
        )

required_cols = [
    "event_time",
    "event_type",
    "price",
    "user_session",
    "product_id",
    "brand",
    "category_code",
    "event_hour",
    "event_dayofweek",
]
missing_cols = [c for c in required_cols if c not in df_clean.columns]
if missing_cols:
    raise ValueError(f"df_clean is missing required columns for P3-T2: {missing_cols}")

df_clean.createOrReplaceTempView("events_clean")

analysis_total_rows = df_clean.count()
analysis_purchase_rows = df_clean.filter(F.col("event_type") == "purchase").count()

print(f"P3-T2 preflight rows: {analysis_total_rows:,}")
print(f"P3-T2 purchase rows: {analysis_purchase_rows:,}")
print("P3-T2 preflight complete.")

# COMMAND ----------

# ANALYSIS 1: Event Type Distribution
event_dist = df_clean.groupBy("event_type").count().orderBy(F.col("count").desc())
event_dist.show(truncate=False)

event_dist_pct = event_dist.withColumn("percentage", F.round((F.col("count") / F.lit(analysis_total_rows)) * 100, 2))
event_dist_pct.show(truncate=False)

purchase_share_row = event_dist_pct.filter(F.col("event_type") == "purchase").select("percentage").first()
purchase_share_pct = float(purchase_share_row["percentage"]) if purchase_share_row else 0.0
print(f"Purchase event share: {purchase_share_pct:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC **Business Interpretation — Analysis 1 (Event Mix)**
# MAGIC The event distribution usually shows a browsing-heavy journey, where views dominate and purchases are much smaller.
# MAGIC A large gap between view activity and purchase activity signals conversion opportunity and supports funnel optimization.
# MAGIC Use the purchase share printed above as the baseline conversion-event indicator for later analyses.

# COMMAND ----------

# ANALYSIS 2: Revenue Analysis
revenue = df_clean.filter(F.col("event_type") == "purchase").agg(F.sum("price").alias("total_revenue"))
revenue.show(truncate=False)

total_revenue_value = revenue.first()["total_revenue"]
total_revenue_value = float(total_revenue_value) if total_revenue_value is not None else 0.0

monthly_revenue = (
    df_clean.filter(F.col("event_type") == "purchase")
    .withColumn("year_month", F.date_format("event_time", "yyyy-MM"))
    .groupBy("year_month")
    .agg(
        F.round(F.sum("price"), 2).alias("total_revenue"),
        F.count("*").alias("num_purchases"),
        F.round(F.avg("price"), 2).alias("avg_purchase_price"),
    )
    .orderBy("year_month")
)
monthly_revenue.show(truncate=False)

top_month = monthly_revenue.orderBy(F.col("total_revenue").desc()).first()
if top_month:
    print(f"Top revenue month: {top_month['year_month']} (${top_month['total_revenue']:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC **Business Interpretation — Analysis 2 (Revenue Trends)**
# MAGIC Total purchase revenue quantifies realized value, while month-by-month trends expose seasonality and campaign timing effects.
# MAGIC The strongest month often reflects high-intent periods (for example, promotional or holiday demand windows).
# MAGIC These results can guide campaign calendars, inventory planning, and budget allocation by month.

# COMMAND ----------

# ANALYSIS 3: Top 10 Most Popular Products (Views vs Purchases)
top_viewed = (
    df_clean.filter(F.col("event_type") == "view")
    .groupBy("product_id", "brand", "category_code")
    .count()
    .orderBy(F.col("count").desc())
    .limit(10)
)
top_viewed.show(truncate=False)

top_purchased = (
    df_clean.filter(F.col("event_type") == "purchase")
    .groupBy("product_id", "brand", "category_code")
    .count()
    .orderBy(F.col("count").desc())
    .limit(10)
)
top_purchased.show(truncate=False)

view_ids = {r["product_id"] for r in top_viewed.select("product_id").collect() if r["product_id"] is not None}
purchase_ids = {r["product_id"] for r in top_purchased.select("product_id").collect() if r["product_id"] is not None}
overlap_count = len(view_ids.intersection(purchase_ids))
print(f"Top-10 overlap between viewed and purchased products: {overlap_count} products")

# COMMAND ----------

# MAGIC %md
# MAGIC **Business Interpretation — Analysis 3 (Product Demand vs Conversion)**
# MAGIC Comparing top-viewed and top-purchased products reveals where attention does not convert into transactions.
# MAGIC Low overlap can indicate friction (price, trust, UX, or stock issues), while high overlap suggests strong product-market fit.
# MAGIC This is a direct signal for merchandising priorities and conversion-focused product page improvements.

# COMMAND ----------

# ANALYSIS 4: Top 10 Brands by Revenue
brand_revenue = (
    df_clean.filter(F.col("event_type") == "purchase")
    .groupBy("brand")
    .agg(
        F.round(F.sum("price"), 2).alias("total_revenue"),
        F.count("*").alias("num_purchases"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
    .orderBy(F.col("total_revenue").desc())
    .limit(10)
)
brand_revenue.show(truncate=False)

top10_brand_revenue = brand_revenue.agg(F.sum("total_revenue").alias("top10_total")).first()["top10_total"]
top10_brand_revenue = float(top10_brand_revenue) if top10_brand_revenue is not None else 0.0
top10_share_pct = (top10_brand_revenue / total_revenue_value) * 100 if total_revenue_value else 0.0
print(f"Top-10 brand revenue share: {top10_share_pct:.2f}% of total revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC **Business Interpretation — Analysis 4 (Brand Concentration)**
# MAGIC Brand-level revenue concentration highlights whether growth depends on a narrow set of brands or a broad portfolio.
# MAGIC A high top-brand revenue share indicates concentration risk but also clear partnership and promotion opportunities.
# MAGIC This supports strategic brand investment, co-marketing, and assortment planning decisions.

# COMMAND ----------

# ANALYSIS 5: Hourly Activity Patterns (Busiest Hours)
hourly = (
    df_clean.groupBy("event_hour")
    .agg(
        F.count("*").alias("total_events"),
        F.count(F.when(F.col("event_type") == "purchase", True)).alias("purchases"),
    )
    .withColumn("purchase_rate_pct", F.round((F.col("purchases") / F.col("total_events")) * 100, 2))
    .orderBy("event_hour")
)
hourly.show(24, truncate=False)

peak_browsing_hour = hourly.orderBy(F.col("total_events").desc()).first()["event_hour"]
peak_purchase_hour = hourly.orderBy(F.col("purchases").desc()).first()["event_hour"]
print(f"Peak browsing hour (UTC): {peak_browsing_hour}")
print(f"Peak purchase hour (UTC): {peak_purchase_hour}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Business Interpretation — Analysis 5 (Hourly Timing)**
# MAGIC Hourly demand patterns identify when users are most active and when buying intent is strongest.
# MAGIC Differences between browsing peaks and purchase peaks can guide campaign scheduling and retargeting windows.
# MAGIC Use peak purchase hours as preferred launch windows for short promotions and alerts.

# COMMAND ----------

# ANALYSIS 6: Day-of-Week Patterns
daily = (
    df_clean.groupBy("event_dayofweek")
    .agg(
        F.count("*").alias("total_events"),
        F.count(F.when(F.col("event_type") == "purchase", True)).alias("purchases"),
    )
    .withColumn("purchase_rate_pct", F.round((F.col("purchases") / F.col("total_events")) * 100, 2))
    .withColumn(
        "day_name",
        F.expr(
            "CASE "
            "WHEN event_dayofweek = 1 THEN 'Sun' "
            "WHEN event_dayofweek = 2 THEN 'Mon' "
            "WHEN event_dayofweek = 3 THEN 'Tue' "
            "WHEN event_dayofweek = 4 THEN 'Wed' "
            "WHEN event_dayofweek = 5 THEN 'Thu' "
            "WHEN event_dayofweek = 6 THEN 'Fri' "
            "WHEN event_dayofweek = 7 THEN 'Sat' "
            "END"
        ),
    )
    .orderBy("event_dayofweek")
)
daily.select("event_dayofweek", "day_name", "total_events", "purchases", "purchase_rate_pct").show(truncate=False)

best_day = daily.orderBy(F.col("purchase_rate_pct").desc()).first()
if best_day:
    print(f"Highest purchase-rate day: {best_day['day_name']} ({best_day['purchase_rate_pct']}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Business Interpretation — Analysis 6 (Weekly Rhythm)**
# MAGIC Weekly patterns reveal whether demand and conversion are concentrated on weekdays or weekends.
# MAGIC High-conversion days are useful for campaign timing, influencer pushes, and offer sequencing.
# MAGIC Aligning spend to high-intent days can improve efficiency without increasing overall budget.

# COMMAND ----------

# ANALYSIS 7: Conversion Funnel (View -> Cart -> Purchase)
funnel_source = "full dataset"
try:
    session_funnel = (
        df_clean.filter(F.col("user_session").isNotNull())
        .groupBy("user_session")
        .agg(
            F.max(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("has_view"),
            F.max(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("has_cart"),
            F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("has_purchase"),
        )
    )
except Exception as e:
    print(f"Full-session funnel failed: {e}")
    print("Retrying funnel on 30% sample...")
    funnel_source = "30% sample"
    sample_df = df_clean.sample(0.3, seed=42)
    session_funnel = (
        sample_df.filter(F.col("user_session").isNotNull())
        .groupBy("user_session")
        .agg(
            F.max(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("has_view"),
            F.max(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("has_cart"),
            F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("has_purchase"),
        )
    )

funnel_counts = session_funnel.agg(
    F.count("*").alias("total_sessions"),
    F.sum("has_view").alias("view_sessions"),
    F.sum("has_cart").alias("cart_sessions"),
    F.sum("has_purchase").alias("purchase_sessions"),
).first()

total_sessions = int(funnel_counts["total_sessions"] or 0)
viewed_sessions = int(funnel_counts["view_sessions"] or 0)
cart_sessions = int(funnel_counts["cart_sessions"] or 0)
purchase_sessions = int(funnel_counts["purchase_sessions"] or 0)

view_rate = (viewed_sessions / total_sessions) * 100 if total_sessions else 0.0
cart_rate = (cart_sessions / total_sessions) * 100 if total_sessions else 0.0
purchase_rate = (purchase_sessions / total_sessions) * 100 if total_sessions else 0.0
view_to_cart_rate = (cart_sessions / viewed_sessions) * 100 if viewed_sessions else 0.0
cart_to_purchase_rate = (purchase_sessions / cart_sessions) * 100 if cart_sessions else 0.0
cart_abandonment_rate = 100 - cart_to_purchase_rate if cart_sessions else 0.0

print(f"Funnel source: {funnel_source}")
print(f"Total Sessions: {total_sessions:,}")
print(f"Sessions with Views: {viewed_sessions:,} ({view_rate:.2f}%)")
print(f"Sessions with Cart: {cart_sessions:,} ({cart_rate:.2f}%)")
print(f"Sessions with Purchase: {purchase_sessions:,} ({purchase_rate:.2f}%)")
print(f"View->Cart Rate: {view_to_cart_rate:.2f}%")
print(f"Cart->Purchase Rate: {cart_to_purchase_rate:.2f}%")
print(f"Cart Abandonment Rate: {cart_abandonment_rate:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC **Business Interpretation — Analysis 7 (Conversion Funnel)**
# MAGIC Session-level funnel metrics expose where value leaks between browsing, carting, and purchasing.
# MAGIC The most actionable KPI is cart abandonment, which directly points to checkout friction and offer-fit issues.
# MAGIC Use this funnel baseline to prioritize interventions such as checkout UX fixes, reminders, and pricing tests.

# COMMAND ----------

# ANALYSIS 8: Average Basket Value
basket = (
    df_clean.filter((F.col("event_type") == "purchase") & F.col("user_session").isNotNull())
    .groupBy("user_session")
    .agg(
        F.round(F.sum("price"), 2).alias("basket_total"),
        F.count("*").alias("items_count"),
    )
)

basket.describe("basket_total", "items_count").show(truncate=False)

basket_kpis = basket.agg(
    F.round(F.avg("basket_total"), 2).alias("avg_basket_value"),
    F.round(F.expr("percentile_approx(basket_total, 0.5)"), 2).alias("median_basket_value"),
    F.round(F.expr("percentile_approx(basket_total, 0.9)"), 2).alias("p90_basket_value"),
    F.round(F.avg("items_count"), 2).alias("avg_items_per_order"),
).first()

print(f"Average basket value: ${basket_kpis['avg_basket_value']}")
print(f"Median basket value: ${basket_kpis['median_basket_value']}")
print(f"90th percentile basket value: ${basket_kpis['p90_basket_value']}")
print(f"Average items per order: {basket_kpis['avg_items_per_order']}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Business Interpretation — Analysis 8 (Basket Economics)**
# MAGIC Basket value and item count summarize transactional quality, not just transaction volume.
# MAGIC Median and high-percentile basket values help distinguish regular orders from high-value outliers.
# MAGIC These metrics support upsell/cross-sell design and threshold-based promotions (for example, free-shipping cutoffs).

# COMMAND ----------

# MAGIC %md
# MAGIC ### P3-T2 Checkpoint Summary
# MAGIC - 8 descriptive analyses were executed from the cleaned dataset.
# MAGIC - Each analysis includes a business interpretation for report integration.
# MAGIC - If session resets occur, rerun the P3-T2 preflight cell first, then continue from Analysis 1.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Visualization (Step 4)
# MAGIC ### P4-T1: Create Visualizations
# MAGIC This section creates five charts from aggregated real data in df_clean.
# MAGIC
# MAGIC Implementation notes:
# MAGIC - Only aggregated Spark outputs are converted to Pandas.
# MAGIC - The full 20M+ row dataset is never converted to Pandas.
# MAGIC - Each chart includes a written interpretation cell for report-ready narrative.

# COMMAND ----------

# P4-T1 preflight: make this section runnable even after session resets.
if "F" not in globals():
    from pyspark.sql import functions as F

if "StringType" not in globals():
    from pyspark.sql.types import StringType

if "reduce" not in globals():
    from functools import reduce

if "spark" not in globals():
    try:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
    except Exception:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

if "refresh_spark_if_needed" not in globals():

    def refresh_spark_if_needed():
        global spark
        try:
            spark.sql("SELECT 1").collect()
            return False
        except Exception:
            try:
                from databricks.connect import DatabricksSession

                spark = DatabricksSession.builder.getOrCreate()
            except Exception:
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
            print("Spark session refreshed.")
            return True


if "load_monthly_dataset" not in globals():

    def load_monthly_dataset(data_path, month_list, log_month_counts=True):
        dfs = []
        for m in month_list:
            try:
                temp = spark.read.csv(f"{data_path}/{m}.csv", header=True, inferSchema=True)
                dfs.append(temp)
                if log_month_counts:
                    print(f"Loaded {m}: {temp.count()} rows")
            except Exception as e:
                print(f"Could not load {m}: {e}")

        if not dfs:
            raise ValueError(f"No datasets could be loaded from {data_path}")

        return reduce(lambda a, b: a.unionByName(b), dfs)


if "DATA_PATH" not in globals():
    DATA_PATH = "/Volumes/workspace/default/cosmetics_data"

if "months" not in globals():
    months = ["2019-Oct", "2019-Nov", "2019-Dec", "2020-Jan", "2020-Feb"]

session_refreshed = refresh_spark_if_needed()
if session_refreshed:
    print("Spark session was refreshed for P4-T1.")

if "df_clean" not in globals():
    print("df_clean not found - rebuilding from source data for visualization section...")

    if "df" not in globals():
        df = load_monthly_dataset(DATA_PATH, months, log_month_counts=True)

    if isinstance(df.schema["event_time"].dataType, StringType):
        df = df.withColumn(
            "event_time",
            F.to_timestamp(F.regexp_replace(F.col("event_time"), " UTC", ""), "yyyy-MM-dd HH:mm:ss"),
        )

    df_clean = df.fillna({"category_code": "unknown", "brand": "unknown"})
    df_clean = df_clean.filter(F.col("event_type").isNotNull())
    df_clean = df_clean.filter(F.col("event_time").isNotNull())
    df_clean = df_clean.filter(F.col("price").isNotNull() & (F.col("price") > 0))

    df_clean = (
        df_clean.withColumn("event_date", F.to_date("event_time"))
        .withColumn("event_hour", F.hour("event_time"))
        .withColumn("event_dayofweek", F.dayofweek("event_time"))
        .withColumn("event_month", F.month("event_time"))
        .withColumn(
            "main_category",
            F.when(F.col("category_code") != "unknown", F.split(F.col("category_code"), "\\.").getItem(0)).otherwise(
                "unknown"
            ),
        )
    )
else:
    print("df_clean found in memory.")

# Backward-compatibility fix for runs that used the old split pattern.
if "main_category" in df_clean.columns:
    legacy_main_category = (
        df_clean.filter(
            (F.col("main_category").isNotNull())
            & (F.col("main_category") != "unknown")
            & F.col("main_category").contains(".")
        )
        .limit(1)
        .count()
    )
    if legacy_main_category > 0:
        print("Detected legacy main_category values. Recomputing from category_code...")
        df_clean = df_clean.withColumn(
            "main_category",
            F.when(F.col("category_code") != "unknown", F.split(F.col("category_code"), "\\.").getItem(0)).otherwise(
                "unknown"
            ),
        )

required_viz_cols = ["event_time", "event_type", "price", "brand", "event_hour", "main_category"]
missing_viz_cols = [c for c in required_viz_cols if c not in df_clean.columns]
if missing_viz_cols:
    raise ValueError(f"df_clean is missing required columns for P4-T1: {missing_viz_cols}")

viz_total_rows = df_clean.count()
viz_purchase_rows = df_clean.filter(F.col("event_type") == "purchase").count()

if viz_purchase_rows == 0:
    raise ValueError("No purchase rows found after cleaning; cannot generate Phase 4 charts.")

print(f"P4-T1 preflight rows: {viz_total_rows:,}")
print(f"P4-T1 purchase rows: {viz_purchase_rows:,}")
print("P4-T1 preflight complete.")

# COMMAND ----------

import matplotlib.pyplot as plt  # noqa: E402
import seaborn as sns  # noqa: E402

sns.set_theme(style="whitegrid")
plt.rcParams["figure.dpi"] = 120
print("Matplotlib, Seaborn, and Pandas imports loaded.")

# COMMAND ----------

# VISUALIZATION 1: BAR CHART — Top 10 Brands by Revenue
brand_rev = (
    df_clean.filter(F.col("event_type") == "purchase")
    .groupBy("brand")
    .agg(F.round(F.sum("price"), 2).alias("revenue"))
    .orderBy(F.col("revenue").desc())
    .limit(10)
)

brand_rev_pd = brand_rev.toPandas().sort_values("revenue", ascending=False)
if brand_rev_pd.empty:
    raise ValueError("Visualization 1 produced no rows. Check purchase and brand data quality.")

total_purchase_revenue = (
    df_clean.filter(F.col("event_type") == "purchase")
    .agg(F.sum("price").alias("total_revenue"))
    .first()["total_revenue"]
)
total_purchase_revenue = float(total_purchase_revenue) if total_purchase_revenue is not None else 0.0

top_brand = brand_rev_pd.iloc[0]["brand"]
top_brand_revenue = float(brand_rev_pd.iloc[0]["revenue"])
top3_share_total = (
    float(brand_rev_pd.head(3)["revenue"].sum()) / total_purchase_revenue * 100 if total_purchase_revenue else 0.0
)
top10_share_total = (
    float(brand_rev_pd["revenue"].sum()) / total_purchase_revenue * 100 if total_purchase_revenue else 0.0
)

plt.figure(figsize=(12, 6))
sns.barplot(data=brand_rev_pd, x="revenue", y="brand", palette="viridis")
plt.title("Top 10 Brands by Revenue", fontsize=16)
plt.xlabel("Total Revenue (USD)")
plt.ylabel("Brand")
plt.tight_layout()
plt.show()

print(f"Top brand by revenue: {top_brand} (${top_brand_revenue:,.2f})")
print(f"Top-3 revenue share of total purchases: {top3_share_total:.2f}%")
print(f"Top-10 revenue share of total purchases: {top10_share_total:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation — Visualization 1 (Top Brands by Revenue)**
# MAGIC The chart highlights which brands generate the highest purchase value, showing how concentrated revenue is across the brand portfolio.
# MAGIC In this project, a high ranking for unknown can appear because missing brand values were intentionally preserved during cleaning to keep behavioral signal.
# MAGIC Use the printed Top-3 and Top-10 revenue share values above to justify brand prioritization and concentration-risk discussion in the report.

# COMMAND ----------

# VISUALIZATION 2: LINE CHART — Monthly Revenue Trend
monthly_rev = (
    df_clean.filter(F.col("event_type") == "purchase")
    .withColumn("year_month", F.date_format("event_time", "yyyy-MM"))
    .groupBy("year_month")
    .agg(F.round(F.sum("price"), 2).alias("revenue"))
    .orderBy("year_month")
)

monthly_rev_pd = monthly_rev.toPandas().sort_values("year_month")
if monthly_rev_pd.empty:
    raise ValueError("Visualization 2 produced no rows. Check purchase timeline data.")

label_map = {
    "2019-10": "Oct 2019",
    "2019-11": "Nov 2019",
    "2019-12": "Dec 2019",
    "2020-01": "Jan 2020",
    "2020-02": "Feb 2020",
}
monthly_rev_pd["month_label"] = monthly_rev_pd["year_month"].map(label_map).fillna(monthly_rev_pd["year_month"])

top_month_idx = monthly_rev_pd["revenue"].idxmax()
bottom_month_idx = monthly_rev_pd["revenue"].idxmin()
top_month_label = monthly_rev_pd.loc[top_month_idx, "month_label"]
bottom_month_label = monthly_rev_pd.loc[bottom_month_idx, "month_label"]

plt.figure(figsize=(10, 5))
plt.plot(
    monthly_rev_pd["month_label"],
    monthly_rev_pd["revenue"],
    marker="o",
    linewidth=2,
    markersize=8,
    color="#2E86AB",
)
plt.title("Monthly Revenue Trend (Oct 2019 - Feb 2020)", fontsize=16)
plt.xlabel("Month")
plt.ylabel("Revenue (USD)")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

print(
    f"Highest revenue month: {top_month_label} (${float(monthly_rev_pd.loc[top_month_idx, 'revenue']):,.2f}); "
    f"Lowest revenue month: {bottom_month_label} (${float(monthly_rev_pd.loc[bottom_month_idx, 'revenue']):,.2f})"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation — Visualization 2 (Monthly Revenue Trend)**
# MAGIC The line chart shows how purchase revenue changes across the five-month period and helps identify seasonality.
# MAGIC Peaks and dips can be mapped to campaign windows, holidays, or post-holiday normalization behavior.
# MAGIC This trend supports business decisions on campaign timing, budget pacing, and inventory planning by month.

# COMMAND ----------

# VISUALIZATION 3: PIE CHART — Event Type Distribution
event_dist = df_clean.groupBy("event_type").count().orderBy(F.col("count").desc())
event_pd = event_dist.toPandas()
if event_pd.empty:
    raise ValueError("Visualization 3 produced no rows. Check event_type data.")

event_total = int(event_pd["count"].sum())
view_count = int(event_pd.loc[event_pd["event_type"] == "view", "count"].sum())
purchase_count = int(event_pd.loc[event_pd["event_type"] == "purchase", "count"].sum())
view_share = (view_count / event_total) * 100 if event_total else 0.0
purchase_share = (purchase_count / event_total) * 100 if event_total else 0.0

colors = ["#2E86AB", "#A23B72", "#F18F01", "#C73E1D"]
plt.figure(figsize=(8, 8))
plt.pie(
    event_pd["count"],
    labels=event_pd["event_type"],
    autopct="%1.1f%%",
    colors=colors[: len(event_pd)],
    startangle=140,
    textprops={"fontsize": 12},
)
plt.title("Distribution of Event Types", fontsize=16)
plt.tight_layout()
plt.show()

print(f"View share: {view_share:.2f}% | Purchase share: {purchase_share:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation — Visualization 3 (Event Type Mix)**
# MAGIC The event distribution confirms a behavior funnel where non-purchase actions dominate overall activity.
# MAGIC The gap between view share and purchase share highlights a conversion challenge typical of e-commerce clickstream data.
# MAGIC This motivates optimization efforts in product pages, cart experience, and checkout flow.

# COMMAND ----------

# VISUALIZATION 4: BAR CHART — Hourly Purchase Pattern
hourly_purchases = (
    df_clean.filter(F.col("event_type") == "purchase").groupBy("event_hour").count().orderBy("event_hour")
)
hourly_pd = hourly_purchases.toPandas()
if hourly_pd.empty:
    raise ValueError("Visualization 4 produced no rows. Check hourly purchase data.")

hourly_pd["event_hour"] = hourly_pd["event_hour"].astype(int)
peak_hour = int(hourly_pd.loc[hourly_pd["count"].idxmax(), "event_hour"])
peak_hour_count = int(hourly_pd["count"].max())

top3_hourly = hourly_pd.sort_values("count", ascending=False).head(3)
top3_hours_str = ", ".join([f"{int(r.event_hour)}:00" for _, r in top3_hourly.iterrows()])

plt.figure(figsize=(14, 6))
sns.barplot(data=hourly_pd, x="event_hour", y="count", palette="RdYlGn_r")
plt.title("Purchase Activity by Hour of Day (UTC)", fontsize=16)
plt.xlabel("Hour of Day")
plt.ylabel("Number of Purchases")
plt.xticks(range(0, 24))
plt.tight_layout()
plt.show()

print(f"Peak purchase hour (UTC): {peak_hour}:00 ({peak_hour_count:,} purchases)")
print(f"Top 3 purchase hours (UTC): {top3_hours_str}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation — Visualization 4 (Hourly Purchase Pattern)**
# MAGIC Hourly purchase distribution identifies when customers are most likely to transact.
# MAGIC Concentrated peak hours are useful for scheduling flash campaigns, reminders, and ad bursts at high-intent times.
# MAGIC The printed top-hour metrics above can be used directly in recommendations for campaign timing.

# COMMAND ----------

# VISUALIZATION 5: BAR CHART — Top 10 Product Categories by Purchase Count
cat_purchases = (
    df_clean.filter((F.col("event_type") == "purchase") & (F.col("main_category") != "unknown"))
    .groupBy("main_category")
    .count()
    .orderBy(F.col("count").desc())
    .limit(10)
)
cat_pd = cat_purchases.toPandas()

if cat_pd.empty:
    print("No non-unknown categories found. Re-running category chart with all categories.")
    cat_purchases = (
        df_clean.filter(F.col("event_type") == "purchase")
        .groupBy("main_category")
        .count()
        .orderBy(F.col("count").desc())
        .limit(10)
    )
    cat_pd = cat_purchases.toPandas()

if cat_pd.empty:
    raise ValueError("Visualization 5 produced no rows. Check main_category data.")

cat_pd = cat_pd.sort_values("count", ascending=False)
top_category = cat_pd.iloc[0]["main_category"]
top_category_share_top10 = float(cat_pd.iloc[0]["count"]) / float(cat_pd["count"].sum()) * 100

plt.figure(figsize=(12, 6))
sns.barplot(data=cat_pd, x="count", y="main_category", palette="coolwarm")
plt.title("Top 10 Product Categories by Purchase Volume", fontsize=16)
plt.xlabel("Number of Purchases")
plt.ylabel("Category")
plt.tight_layout()
plt.show()

print(f"Top purchase category: {top_category}")
print(f"Top category share within top-10 categories: {top_category_share_top10:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation — Visualization 5 (Category Purchase Concentration)**
# MAGIC Category-level purchase distribution shows where transactional demand is strongest in the catalog.
# MAGIC This is useful for inventory prioritization, category-level promotions, and merchandising focus.
# MAGIC The top-category share printed above quantifies how concentrated purchases are among the leading categories.

# COMMAND ----------

# MAGIC %md
# MAGIC ### P4-T1 Checkpoint Summary
# MAGIC - 5 visualizations created from real aggregated data in df_clean.
# MAGIC - Chart types used: bar, line, and pie.
# MAGIC - Every chart includes titles/labels and a written interpretation.
# MAGIC - Pandas conversion is limited to small aggregate result sets only.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Predictive Analytics — Machine Learning (Step 5)
# MAGIC ### P5-T1: Feature Engineering for ML
# MAGIC
# MAGIC Build session-level features for binary classification:
# MAGIC - Prediction unit: `user_session`
# MAGIC - Label: `1` if the session contains a purchase event, otherwise `0`
# MAGIC - Feature set: behavioral, product-diversity, price, and session-timing metrics

# COMMAND ----------

if "df_clean" not in globals():
    raise ValueError("df_clean is not available. Run the Step 3 cleaning section before Phase 5.")

required_ml_source_cols = ["user_session", "event_type", "product_id", "brand", "price", "event_time"]
missing_ml_source_cols = [c for c in required_ml_source_cols if c not in df_clean.columns]
if missing_ml_source_cols:
    raise ValueError(f"df_clean is missing required columns for P5-T1: {missing_ml_source_cols}")

# Label from full session outcome.
base_events = df_clean.filter(F.col("user_session").isNotNull()).select(
    "user_session", "event_time", "event_type", "product_id", "brand", "price"
)

session_labels = base_events.groupBy("user_session").agg(
    F.max(F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))).alias("label")
)

# First purchase timestamp per session (only for positive sessions).
first_purchase_ts = (
    base_events.filter(F.col("event_type") == "purchase")
    .groupBy("user_session")
    .agg(F.min("event_time").alias("first_purchase_ts"))
)

# Leakage-safe feature rows: non-purchase events before first purchase (or all non-purchase events for negative sessions).
feature_event_rows = (
    base_events.join(first_purchase_ts, on="user_session", how="left")
    .filter(F.col("event_type") != "purchase")
    .filter(F.col("first_purchase_ts").isNull() | (F.col("event_time") < F.col("first_purchase_ts")))
)

session_feature_values = feature_event_rows.groupBy("user_session").agg(
    F.count("*").alias("total_events"),
    F.sum(F.when(F.col("event_type") == "view", F.lit(1)).otherwise(F.lit(0))).alias("view_count"),
    F.sum(F.when(F.col("event_type") == "cart", F.lit(1)).otherwise(F.lit(0))).alias("cart_count"),
    F.sum(F.when(F.col("event_type") == "remove_from_cart", F.lit(1)).otherwise(F.lit(0))).alias("remove_cart_count"),
    F.countDistinct(F.when(F.col("event_type") == "view", F.col("product_id"))).alias("unique_products_viewed"),
    F.countDistinct("brand").alias("unique_brands"),
    F.avg("price").alias("avg_price"),
    F.max("price").alias("max_price"),
    F.min("price").alias("min_price"),
    F.min("event_time").alias("session_start_ts"),
)

session_features = (
    session_labels.join(session_feature_values, on="user_session", how="left")
    .withColumn("session_start_hour", F.hour("session_start_ts"))
    .withColumn("day_of_week", F.dayofweek("session_start_ts"))
    .drop("session_start_ts", "first_purchase_ts")
    .fillna(
        {
            "total_events": 0,
            "view_count": 0,
            "cart_count": 0,
            "remove_cart_count": 0,
            "unique_products_viewed": 0,
            "unique_brands": 0,
            "avg_price": 0.0,
            "max_price": 0.0,
            "min_price": 0.0,
            "session_start_hour": 0,
            "day_of_week": 0,
        }
    )
    .withColumn("label", F.col("label").cast("double"))
)

feature_columns = [
    "total_events",
    "view_count",
    "cart_count",
    "remove_cart_count",
    "unique_products_viewed",
    "unique_brands",
    "avg_price",
    "max_price",
    "min_price",
    "session_start_hour",
    "day_of_week",
]

session_counts = session_features.agg(
    F.count("*").alias("total_sessions"),
    F.sum("label").alias("purchase_sessions"),
).first()

total_sessions = int(session_counts["total_sessions"] or 0)
purchase_sessions = int(session_counts["purchase_sessions"] or 0)
non_purchase_sessions = total_sessions - purchase_sessions
positive_class_pct = (purchase_sessions / total_sessions) * 100 if total_sessions else 0.0

print(f"Total sessions: {total_sessions:,}")
print(f"Purchase sessions (label=1): {purchase_sessions:,}")
print(f"Non-purchase sessions (label=0): {non_purchase_sessions:,}")
print(f"Class balance: {positive_class_pct:.2f}% positive (purchase)")

session_features.select(["user_session", "label"] + feature_columns).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### P5-T1: Class Imbalance Visualization
# MAGIC Visualize the binary target distribution to make minority-class skew explicit before model training.

# COMMAND ----------

import matplotlib.pyplot as plt  # noqa: E402
import seaborn as sns  # noqa: E402

label_distribution_pd = (
    session_features.groupBy("label")
    .count()
    .withColumn("label_int", F.col("label").cast("int"))
    .orderBy("label_int")
    .toPandas()
)

if label_distribution_pd.empty:
    raise ValueError("Label distribution is empty; cannot visualize class imbalance.")

label_distribution_pd["label_name"] = label_distribution_pd["label_int"].map({0: "Non-purchase (0)", 1: "Purchase (1)"})
label_distribution_pd["percentage"] = (label_distribution_pd["count"] / label_distribution_pd["count"].sum()) * 100

sns.set_theme(style="whitegrid")
plt.figure(figsize=(8, 5))
ax = sns.barplot(
    data=label_distribution_pd,
    x="label_name",
    y="count",
    palette=["#4C72B0", "#DD8452"],
)

for i, row in label_distribution_pd.reset_index(drop=True).iterrows():
    ax.text(
        i,
        float(row["count"]),
        f"{int(row['count']):,}\n({float(row['percentage']):.2f}%)",
        ha="center",
        va="bottom",
        fontsize=10,
    )

plt.title("Session-Level Class Distribution (Target Label)", fontsize=14)
plt.xlabel("Class")
plt.ylabel("Number of Sessions")
plt.tight_layout()
plt.show()

imbalance_ratio = (non_purchase_sessions / purchase_sessions) if purchase_sessions else float("inf")
print(f"Imbalance ratio (label 0 : label 1): {imbalance_ratio:.2f} : 1")

# COMMAND ----------


from pyspark.ml.linalg import Vectors, VectorUDT  # noqa: E402
from pyspark.sql.types import DoubleType  # noqa: E402


# Create a UDF to assemble features into a vector
def make_vector(*args):
    return Vectors.dense(args)


make_vector_udf = F.udf(make_vector, VectorUDT())

# Apply the UDF to create feature vectors
ml_data = session_features.withColumn(
    "features", make_vector_udf(*[F.col(c).cast(DoubleType()) for c in feature_columns])
).select("features", "label")

ml_rows = ml_data.count()
dropped_after_assembly = total_sessions - ml_rows

print(f"Rows after feature vector assembly: {ml_rows:,}")
print(f"Rows dropped during vector assembly: {dropped_after_assembly:,}")

ml_data.show(5, truncate=False)

# COMMAND ----------

train_data, test_data = ml_data.randomSplit([0.8, 0.2], seed=42)

train_count = train_data.count()
test_count = test_data.count()
train_pct = (train_count / ml_rows) * 100 if ml_rows else 0.0
test_pct = (test_count / ml_rows) * 100 if ml_rows else 0.0

print(f"Training set: {train_count:,} rows ({train_pct:.2f}%)")
print(f"Test set: {test_count:,} rows ({test_pct:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### P5-T1 Checkpoint Summary
# MAGIC - Session-level feature table created with 11 numeric predictors + 1 binary label.
# MAGIC - Class imbalance was measured, printed, and visualized for model-evaluation context.
# MAGIC - Features are built from pre-purchase non-purchase behavior to reduce target leakage risk.
# MAGIC - Features were assembled into Spark ML vectors using manual UDF-based vector construction for runtime compatibility.
# MAGIC - Reproducible train/test split completed with `seed=42`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### P5-T2: Train and Compare Multiple Models
# MAGIC
# MAGIC This section trains and compares:
# MAGIC 1. Logistic Regression
# MAGIC 2. Decision Tree
# MAGIC 3. Random Forest
# MAGIC
# MAGIC Evaluation uses the same metric set for all models:
# MAGIC - Accuracy
# MAGIC - Precision (positive class)
# MAGIC - Recall (positive class)
# MAGIC - F1 Score (positive class)
# MAGIC - AUC-ROC
# MAGIC - AUC-PR

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier, LogisticRegression, RandomForestClassifier  # noqa: E402
from pyspark.ml.evaluation import BinaryClassificationEvaluator  # noqa: E402
from pyspark.ml.functions import vector_to_array  # noqa: E402
from pyspark.ml.linalg import VectorUDT as LocalVectorUDT, Vectors as LocalVectors  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window as LocalWindow  # noqa: E402
import pandas as pd  # noqa: E402

if "train_data" not in globals() or "test_data" not in globals():
    raise ValueError("train_data/test_data not found. Run P5-T1 first.")

print(f"Training rows available: {train_data.count():,}")
print(f"Test rows available: {test_data.count():,}")

# Split training data for model fitting and threshold tuning.
model_train_data, validation_data = train_data.randomSplit([0.85, 0.15], seed=42)

model_train_rows = model_train_data.count()
validation_rows = validation_data.count()

base_model_train_data = model_train_data.select("features", "label")

label_counts = model_train_data.groupBy("label").count().collect()
label_count_map = {float(r["label"]): int(r["count"]) for r in label_counts}

positive_count = label_count_map.get(1.0, 0)
negative_count = label_count_map.get(0.0, 0)
total_for_fit = positive_count + negative_count

weight_pos = (total_for_fit / (2 * positive_count)) if positive_count else 1.0
weight_neg = (total_for_fit / (2 * negative_count)) if negative_count else 1.0

weighted_model_train_data = model_train_data.withColumn(
    "class_weight",
    F.when(F.col("label") == 1.0, F.lit(weight_pos)).otherwise(F.lit(weight_neg)),
)

print(f"Model-fit rows: {model_train_rows:,}")
print(f"Validation rows: {validation_rows:,}")
print(f"Class weights -> label 1: {weight_pos:.4f}, label 0: {weight_neg:.4f}")

# 1) Logistic Regression (class-weighted)
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100, weightCol="class_weight")
lr_model = lr.fit(weighted_model_train_data)
lr_val_raw = lr_model.transform(validation_data)
lr_test_raw = lr_model.transform(test_data)

print("=== Logistic Regression Raw Results ===")
lr_test_raw.select("label", "prediction", "probability").show(10, truncate=False)

# 2) Decision Tree (class-weighted)
dt = DecisionTreeClassifier(featuresCol="features", labelCol="label", maxDepth=10, weightCol="class_weight")
dt_model = dt.fit(weighted_model_train_data)
dt_val_raw = dt_model.transform(validation_data)
dt_test_raw = dt_model.transform(test_data)

print("=== Decision Tree Raw Results ===")
dt_test_raw.select("label", "prediction", "probability").show(10, truncate=False)


# 3) Random Forest (class-weighted)
rf = RandomForestClassifier(
    featuresCol="features", labelCol="label", numTrees=50, maxDepth=10, weightCol="class_weight"
)
rf_model = rf.fit(weighted_model_train_data)
rf_val_raw = rf_model.transform(validation_data)
rf_test_raw = rf_model.transform(test_data)

print("=== Random Forest Raw Results ===")
rf_test_raw.select("label", "prediction", "probability").show(10, truncate=False)

# COMMAND ----------

binary_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")


def apply_threshold(predictions, threshold):
    positive_probability = vector_to_array(F.col("probability")).getItem(1)
    return predictions.withColumn(
        "prediction",
        F.when(positive_probability >= F.lit(float(threshold)), F.lit(1.0)).otherwise(F.lit(0.0)),
    )


def compute_metrics(predictions, include_auc=True):
    auc_roc = None
    auc_pr = None
    if include_auc:
        auc_roc = binary_eval.evaluate(predictions, {binary_eval.metricName: "areaUnderROC"})
        auc_pr = binary_eval.evaluate(predictions, {binary_eval.metricName: "areaUnderPR"})

    cm = predictions.agg(
        F.sum(F.when((F.col("label") == 1.0) & (F.col("prediction") == 1.0), 1).otherwise(0)).alias("tp"),
        F.sum(F.when((F.col("label") == 0.0) & (F.col("prediction") == 1.0), 1).otherwise(0)).alias("fp"),
        F.sum(F.when((F.col("label") == 0.0) & (F.col("prediction") == 0.0), 1).otherwise(0)).alias("tn"),
        F.sum(F.when((F.col("label") == 1.0) & (F.col("prediction") == 0.0), 1).otherwise(0)).alias("fn"),
    ).first()

    tp = int(cm["tp"] or 0)
    fp = int(cm["fp"] or 0)
    tn = int(cm["tn"] or 0)
    fn = int(cm["fn"] or 0)
    total = tp + fp + tn + fn

    accuracy = (tp + tn) / total if total else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) else 0.0

    return {
        "Accuracy": accuracy,
        "Precision": precision,
        "Recall": recall,
        "F1 Score": f1,
        "AUC-ROC": auc_roc,
        "AUC-PR": auc_pr,
        "TP": tp,
        "FP": fp,
        "TN": tn,
        "FN": fn,
    }


MIN_RECALL_TARGET = 0.75
THRESHOLD_GRID = [i / 100 for i in range(5, 96, 5)]


def tune_threshold(validation_predictions, model_name, min_recall_target=MIN_RECALL_TARGET, threshold_grid=None):
    if threshold_grid is None:
        threshold_grid = THRESHOLD_GRID

    rows = []
    for threshold in threshold_grid:
        tuned = apply_threshold(validation_predictions, threshold)
        m = compute_metrics(tuned, include_auc=False)
        rows.append(
            {
                "threshold": threshold,
                "f1": m["F1 Score"],
                "precision": m["Precision"],
                "recall": m["Recall"],
            }
        )

    tuning_pd = pd.DataFrame(rows)
    tuning_display = tuning_pd.sort_values(by=["threshold"])
    eligible = tuning_pd[tuning_pd["recall"] >= float(min_recall_target)]

    if not eligible.empty:
        ranked = eligible.sort_values(
            by=["precision", "f1", "recall", "threshold"],
            ascending=[False, False, False, False],
        )
        best = ranked.iloc[0]
        selection_rule = f"max precision with recall >= {min_recall_target:.2f}"
    else:
        ranked = tuning_pd.sort_values(by=["f1", "recall", "precision"], ascending=False)
        best = ranked.iloc[0]
        selection_rule = "fallback: max F1 (no threshold met recall floor)"

    print(f"\n=== {model_name} Threshold Tuning (Validation) ===")
    print(tuning_display.to_string(index=False))
    print(
        f"Selected threshold for {model_name}: {best['threshold']:.2f} "
        f"(rule={selection_rule}, Precision={best['precision']:.4f}, "
        f"Recall={best['recall']:.4f}, F1={best['f1']:.4f})"
    )

    return float(best["threshold"])


lr_threshold = tune_threshold(lr_val_raw, "Logistic Regression")
dt_threshold = tune_threshold(dt_val_raw, "Decision Tree")
rf_threshold = tune_threshold(rf_val_raw, "Random Forest")

lr_predictions = apply_threshold(lr_test_raw, lr_threshold)
dt_predictions = apply_threshold(dt_test_raw, dt_threshold)
rf_predictions = apply_threshold(rf_test_raw, rf_threshold)


def evaluate_model(predictions, model_name, threshold):
    m = compute_metrics(predictions)

    print(f"\n{'=' * 50}")
    print(f"Model: {model_name}")
    print(f"{'=' * 50}")
    print(f"Threshold: {threshold:.2f}")
    print(f"Accuracy:  {m['Accuracy']:.4f}")
    print(f"Precision (positive): {m['Precision']:.4f}")
    print(f"Recall (positive):    {m['Recall']:.4f}")
    print(f"F1 Score (positive):  {m['F1 Score']:.4f}")
    print(f"AUC-ROC:              {m['AUC-ROC']:.4f}")
    print(f"AUC-PR:               {m['AUC-PR']:.4f}")
    print(f"Confusion Matrix: TP={m['TP']:,}, FP={m['FP']:,}, TN={m['TN']:,}, FN={m['FN']:,}")

    return {
        "Model": model_name,
        "Threshold": round(threshold, 2),
        "Accuracy": round(m["Accuracy"], 4),
        "Precision": round(m["Precision"], 4),
        "Recall": round(m["Recall"], 4),
        "F1 Score": round(m["F1 Score"], 4),
        "AUC-ROC": round(m["AUC-ROC"], 4),
        "AUC-PR": round(m["AUC-PR"], 4),
        "TP": m["TP"],
        "FP": m["FP"],
        "TN": m["TN"],
        "FN": m["FN"],
    }


lr_metrics = evaluate_model(lr_predictions, "Logistic Regression", lr_threshold)
dt_metrics = evaluate_model(dt_predictions, "Decision Tree", dt_threshold)
rf_metrics = evaluate_model(rf_predictions, "Random Forest", rf_threshold)

model_metrics = [lr_metrics, dt_metrics, rf_metrics]

comparison_df = spark.createDataFrame(model_metrics)
comparison_df.show(truncate=False)

comparison_pd = pd.DataFrame(model_metrics).sort_values(by=["AUC-PR", "F1 Score"], ascending=False)
print("\n=== MODEL COMPARISON TABLE ===")
print(comparison_pd.to_string(index=False))

best_model_row = comparison_pd.iloc[0]
best_model_name = best_model_row["Model"]

print(f"\nBest model by AUC-PR (tie-break: F1): {best_model_name}")
print("Note: due to class imbalance, prioritize F1, AUC-PR, and confusion matrix over raw accuracy.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### P5-T2b: Imbalance Handling Strategy Experiments (SMOTE + Manual Methods)
# MAGIC
# MAGIC This extension compares Random Forest under four imbalance strategies:
# MAGIC 1. Class-weighted baseline (existing approach)
# MAGIC 2. Manual random undersampling of majority class
# MAGIC 3. Manual random oversampling of minority class
# MAGIC 4. SMOTE-style synthetic minority generation (Spark-native interpolation)
# MAGIC
# MAGIC The same validation split, threshold tuning routine, and test metrics are used to keep comparisons fair.

# COMMAND ----------


IMBALANCE_RUN_PROFILE = "free-tier"  # Options: "free-tier", "full"

if IMBALANCE_RUN_PROFILE == "free-tier":
    STRATEGY_NUM_TREES = 35
    TARGET_POSITIVE_SHARE = 0.17
    RUN_MANUAL_OVERSAMPLE = False
    RUN_SMOTE_LIKE = True
    SMOTE_BUCKET_COUNT = 128
    RF_TUNING_GRID = [
        {
            "numTrees": 60,
            "maxDepth": 8,
            "minInstancesPerNode": 1,
            "subsamplingRate": 0.8,
            "featureSubsetStrategy": "sqrt",
        },
        {
            "numTrees": 90,
            "maxDepth": 10,
            "minInstancesPerNode": 1,
            "subsamplingRate": 0.85,
            "featureSubsetStrategy": "sqrt",
        },
    ]
else:
    STRATEGY_NUM_TREES = 50
    TARGET_POSITIVE_SHARE = 0.20
    RUN_MANUAL_OVERSAMPLE = True
    RUN_SMOTE_LIKE = True
    SMOTE_BUCKET_COUNT = 256
    RF_TUNING_GRID = [
        {
            "numTrees": 80,
            "maxDepth": 8,
            "minInstancesPerNode": 1,
            "subsamplingRate": 0.8,
            "featureSubsetStrategy": "sqrt",
        },
        {
            "numTrees": 120,
            "maxDepth": 8,
            "minInstancesPerNode": 1,
            "subsamplingRate": 0.8,
            "featureSubsetStrategy": "sqrt",
        },
        {
            "numTrees": 120,
            "maxDepth": 10,
            "minInstancesPerNode": 1,
            "subsamplingRate": 0.85,
            "featureSubsetStrategy": "sqrt",
        },
        {
            "numTrees": 160,
            "maxDepth": 12,
            "minInstancesPerNode": 2,
            "subsamplingRate": 0.85,
            "featureSubsetStrategy": "sqrt",
        },
    ]

print(
    "Imbalance experiment profile: "
    f"{IMBALANCE_RUN_PROFILE} | trees={STRATEGY_NUM_TREES} | "
    f"oversample={RUN_MANUAL_OVERSAMPLE} | smote_like={RUN_SMOTE_LIKE}"
)


def limit_rows(df, n_rows, seed):
    if n_rows <= 0:
        return df.limit(0)

    total_rows = df.count()
    if total_rows <= n_rows:
        return df

    sample_fraction = min(1.0, max(0.001, (n_rows / total_rows) * 1.10))
    sampled = df.sample(withReplacement=False, fraction=sample_fraction, seed=seed)
    sampled_count = sampled.count()

    if sampled_count >= n_rows:
        return sampled.limit(int(n_rows))

    deficit = int(n_rows - sampled_count)
    top_up = df.sample(withReplacement=True, fraction=max(0.001, deficit / total_rows), seed=seed + 1).limit(deficit)
    return sampled.unionByName(top_up).limit(int(n_rows))


def summarize_label_mix(df, strategy_name):
    counts = df.groupBy("label").count().collect()
    count_map = {float(r["label"]): int(r["count"]) for r in counts}
    pos = count_map.get(1.0, 0)
    neg = count_map.get(0.0, 0)
    total = pos + neg
    pos_pct = (pos / total) * 100 if total else 0.0
    print(f"{strategy_name:<26s} -> rows={total:,}, pos={pos:,}, neg={neg:,}, pos%={pos_pct:.2f}")


def build_manual_undersample(train_df, target_positive_share=0.20, seed=42):
    positive_df = train_df.filter(F.col("label") == 1.0).select("features", "label")
    negative_df = train_df.filter(F.col("label") == 0.0).select("features", "label")

    pos_count = positive_df.count()
    neg_count = negative_df.count()
    if pos_count == 0 or neg_count == 0:
        return train_df.select("features", "label")

    desired_neg_count = int((pos_count * (1 - target_positive_share)) / target_positive_share)
    if desired_neg_count >= neg_count:
        return train_df.select("features", "label")

    sample_fraction = desired_neg_count / neg_count
    sampled_negatives = negative_df.sample(withReplacement=False, fraction=sample_fraction, seed=seed)
    sampled_negatives = limit_rows(sampled_negatives, desired_neg_count, seed + 1)

    return positive_df.unionByName(sampled_negatives).select("features", "label")


def build_manual_oversample(train_df, target_positive_share=0.20, seed=42):
    positive_df = train_df.filter(F.col("label") == 1.0).select("features", "label")
    negative_df = train_df.filter(F.col("label") == 0.0).select("features", "label")

    pos_count = positive_df.count()
    neg_count = negative_df.count()
    if pos_count == 0 or neg_count == 0:
        return train_df.select("features", "label")

    desired_pos_count = int((neg_count * target_positive_share) / (1 - target_positive_share))
    additional_needed = max(0, desired_pos_count - pos_count)
    if additional_needed == 0:
        return train_df.select("features", "label")

    sample_fraction = max(1.0, (additional_needed / pos_count) * 1.2)
    sampled_positives = positive_df.sample(withReplacement=True, fraction=sample_fraction, seed=seed + 10)
    sampled_positives = limit_rows(sampled_positives, additional_needed, seed + 11)

    oversampled_positives = positive_df.unionByName(sampled_positives)
    return negative_df.unionByName(oversampled_positives).select("features", "label")


def smote_interpolate(vec_a, vec_b, alpha):
    arr_a = vec_a.toArray().tolist()
    arr_b = vec_b.toArray().tolist()
    mixed = [x + float(alpha) * (y - x) for x, y in zip(arr_a, arr_b)]
    return LocalVectors.dense(mixed)


smote_interpolate_udf = F.udf(smote_interpolate, LocalVectorUDT())


def add_random_bucket(df, seed, bucket_count=SMOTE_BUCKET_COUNT):
    return df.withColumn("_bucket", F.floor(F.rand(seed) * F.lit(bucket_count)).cast("int"))


def generate_smote_batch(minority_df, seed):
    left_window = LocalWindow.partitionBy("_bucket").orderBy(F.rand(seed + 2))
    right_window = LocalWindow.partitionBy("_bucket").orderBy(F.rand(seed + 3))

    left = (
        add_random_bucket(minority_df, seed)
        .withColumn("_rn", F.row_number().over(left_window))
        .select("_bucket", "_rn", F.col("features").alias("features_left"))
    )
    right = (
        add_random_bucket(minority_df, seed + 1)
        .withColumn("_rn", F.row_number().over(right_window))
        .select("_bucket", "_rn", F.col("features").alias("features_right"))
    )

    return (
        left.join(right, on=["_bucket", "_rn"], how="inner")
        .drop("_bucket", "_rn")
        .withColumn("alpha", F.rand(seed + 4))
        .withColumn(
            "features",
            smote_interpolate_udf(F.col("features_left"), F.col("features_right"), F.col("alpha")),
        )
        .withColumn("label", F.lit(1.0))
        .select("features", "label")
    )


def build_smote_like_oversample(train_df, target_positive_share=0.20, seed=42):
    positive_df = train_df.filter(F.col("label") == 1.0).select("features", "label")
    negative_df = train_df.filter(F.col("label") == 0.0).select("features", "label")

    pos_count = positive_df.count()
    neg_count = negative_df.count()
    if pos_count < 2 or neg_count == 0:
        return train_df.select("features", "label")

    desired_pos_count = int((neg_count * target_positive_share) / (1 - target_positive_share))
    synthetic_needed = max(0, desired_pos_count - pos_count)
    if synthetic_needed == 0:
        return train_df.select("features", "label")

    full_rounds = synthetic_needed // pos_count
    remainder = synthetic_needed % pos_count

    synthetic_df = None
    for round_idx in range(full_rounds):
        batch_seed = seed + (round_idx * 17)
        batch_df = generate_smote_batch(positive_df, batch_seed)
        synthetic_df = batch_df if synthetic_df is None else synthetic_df.unionByName(batch_df)

    if remainder > 0:
        remainder_df = generate_smote_batch(positive_df, seed + 999)
        remainder_df = limit_rows(remainder_df, remainder, seed + 1000)
        synthetic_df = remainder_df if synthetic_df is None else synthetic_df.unionByName(remainder_df)

    return train_df.select("features", "label").unionByName(synthetic_df.select("features", "label"))


target_positive_share = TARGET_POSITIVE_SHARE
print(f"\nTarget positive share for manual/SMOTE strategies: {target_positive_share:.0%}")

weighted_strategy_train = weighted_model_train_data.select("features", "label", "class_weight")
manual_strategy_base = base_model_train_data.select("features", "label")

manual_undersample_train = build_manual_undersample(manual_strategy_base, target_positive_share, seed=42)
manual_oversample_train = None
smote_like_train = None

if RUN_MANUAL_OVERSAMPLE:
    manual_oversample_train = build_manual_oversample(manual_strategy_base, target_positive_share, seed=42)

if RUN_SMOTE_LIKE:
    smote_like_train = build_smote_like_oversample(manual_strategy_base, target_positive_share, seed=42)

_ = weighted_strategy_train.count()
_ = manual_strategy_base.count()
_ = manual_undersample_train.count()

if manual_oversample_train is not None:
    _ = manual_oversample_train.count()

if smote_like_train is not None:
    _ = smote_like_train.count()

print("Serverless note: strategy datasets were materialized without cache (persist is unsupported).")

print("\n=== Training Label Mix By Strategy ===")
summarize_label_mix(weighted_strategy_train.select("label"), "Class Weighting")
summarize_label_mix(manual_undersample_train, "Manual Undersample")

if manual_oversample_train is not None:
    summarize_label_mix(manual_oversample_train, "Manual Oversample")

if smote_like_train is not None:
    summarize_label_mix(smote_like_train, "SMOTE-Like Synthetic")


def train_rf_with_strategy(strategy_name, train_df, use_class_weights=False, num_trees=STRATEGY_NUM_TREES):
    if use_class_weights:
        rf_classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            numTrees=int(num_trees),
            maxDepth=10,
            weightCol="class_weight",
        )
    else:
        rf_classifier = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            numTrees=int(num_trees),
            maxDepth=10,
        )

    strategy_model = rf_classifier.fit(train_df)
    val_predictions = strategy_model.transform(validation_data)
    test_predictions_raw = strategy_model.transform(test_data)

    selected_threshold = tune_threshold(val_predictions, f"Random Forest [{strategy_name}]")
    test_predictions = apply_threshold(test_predictions_raw, selected_threshold)
    metrics = evaluate_model(test_predictions, f"Random Forest [{strategy_name}]", selected_threshold)
    metrics["Imbalance Strategy"] = strategy_name

    return metrics, strategy_model


def build_rf_classifier(params, use_class_weights=False):
    rf_kwargs = {
        "featuresCol": "features",
        "labelCol": "label",
        "numTrees": int(params["numTrees"]),
        "maxDepth": int(params["maxDepth"]),
        "minInstancesPerNode": int(params["minInstancesPerNode"]),
        "subsamplingRate": float(params["subsamplingRate"]),
        "featureSubsetStrategy": params["featureSubsetStrategy"],
    }
    if use_class_weights:
        rf_kwargs["weightCol"] = "class_weight"
    return RandomForestClassifier(**rf_kwargs)


def tune_rf_hyperparameters(train_df, strategy_name, use_class_weights=False):
    rf_param_grid = RF_TUNING_GRID

    tuning_rows = []
    best_model = None
    best_params = None
    best_auc_pr = -1.0
    best_auc_roc = -1.0

    print(f"\n=== RF Hyperparameter Tuning [{strategy_name}] ===")
    for idx, params in enumerate(rf_param_grid, start=1):
        print(
            "Trying config "
            f"{idx}/{len(rf_param_grid)}: "
            f"numTrees={params['numTrees']}, maxDepth={params['maxDepth']}, "
            f"minInstancesPerNode={params['minInstancesPerNode']}, "
            f"subsamplingRate={params['subsamplingRate']}, "
            f"featureSubsetStrategy={params['featureSubsetStrategy']}"
        )

        rf_candidate = build_rf_classifier(params, use_class_weights=use_class_weights)
        candidate_model = rf_candidate.fit(train_df)
        candidate_val = candidate_model.transform(validation_data)

        auc_pr = binary_eval.evaluate(candidate_val, {binary_eval.metricName: "areaUnderPR"})
        auc_roc = binary_eval.evaluate(candidate_val, {binary_eval.metricName: "areaUnderROC"})

        tuning_rows.append(
            {
                "numTrees": params["numTrees"],
                "maxDepth": params["maxDepth"],
                "minInstancesPerNode": params["minInstancesPerNode"],
                "subsamplingRate": params["subsamplingRate"],
                "featureSubsetStrategy": params["featureSubsetStrategy"],
                "AUC-PR": round(float(auc_pr), 4),
                "AUC-ROC": round(float(auc_roc), 4),
            }
        )

        if (auc_pr > best_auc_pr) or (abs(auc_pr - best_auc_pr) < 1e-12 and auc_roc > best_auc_roc):
            best_auc_pr = float(auc_pr)
            best_auc_roc = float(auc_roc)
            best_model = candidate_model
            best_params = params

    tuning_pd = pd.DataFrame(tuning_rows).sort_values(by=["AUC-PR", "AUC-ROC"], ascending=False)
    print("\nHyperparameter tuning summary (validation):")
    print(tuning_pd.to_string(index=False))
    print(
        "Selected RF hyperparameters: "
        f"numTrees={best_params['numTrees']}, maxDepth={best_params['maxDepth']}, "
        f"minInstancesPerNode={best_params['minInstancesPerNode']}, "
        f"subsamplingRate={best_params['subsamplingRate']}, "
        f"featureSubsetStrategy={best_params['featureSubsetStrategy']} "
        f"(AUC-PR={best_auc_pr:.4f}, AUC-ROC={best_auc_roc:.4f})"
    )

    return best_model, best_params, tuning_pd


def format_rf_params(params):
    return (
        f"numTrees={params['numTrees']}, maxDepth={params['maxDepth']}, "
        f"minInstancesPerNode={params['minInstancesPerNode']}, "
        f"subsamplingRate={params['subsamplingRate']}, "
        f"featureSubsetStrategy={params['featureSubsetStrategy']}"
    )


rf_strategy_results = []
rf_strategy_models = {}

strategy_specs = [
    ("Class Weighting", weighted_strategy_train, True),
    ("Manual Undersample", manual_undersample_train, False),
]

if manual_oversample_train is not None:
    strategy_specs.append(("Manual Oversample", manual_oversample_train, False))

if smote_like_train is not None:
    strategy_specs.append(("SMOTE-Like Synthetic", smote_like_train, False))

strategy_train_map = {name: (train_df, use_weights) for name, train_df, use_weights in strategy_specs}

for strategy_name, train_df, use_weights in strategy_specs:
    strategy_metrics, strategy_model = train_rf_with_strategy(
        strategy_name,
        train_df,
        use_class_weights=use_weights,
        num_trees=STRATEGY_NUM_TREES,
    )
    rf_strategy_results.append(strategy_metrics)
    rf_strategy_models[strategy_name] = strategy_model

rf_strategy_df = spark.createDataFrame(rf_strategy_results)
print("\n=== RANDOM FOREST IMBALANCE STRATEGY COMPARISON ===")
rf_strategy_df.orderBy(F.desc("AUC-PR"), F.desc("F1 Score")).show(truncate=False)

rf_strategy_pd = pd.DataFrame(rf_strategy_results).sort_values(by=["AUC-PR", "F1 Score"], ascending=False)
print(rf_strategy_pd.to_string(index=False))

best_strategy_row = rf_strategy_pd.iloc[0]
best_strategy_name = best_strategy_row["Imbalance Strategy"]
print(f"\nBest Random Forest imbalance strategy by AUC-PR (tie-break: F1): {best_strategy_name}")

print("\n=== Targeted RF Hyperparameter Tuning On Best Imbalance Strategy ===")
best_strategy_train_df, best_strategy_use_weights = strategy_train_map[best_strategy_name]
tuned_rf_model, tuned_rf_params, _ = tune_rf_hyperparameters(
    best_strategy_train_df,
    best_strategy_name,
    use_class_weights=best_strategy_use_weights,
)

tuned_val_raw = tuned_rf_model.transform(validation_data)
tuned_threshold = tune_threshold(tuned_val_raw, f"Random Forest [{best_strategy_name}] Tuned")
tuned_test_raw = tuned_rf_model.transform(test_data)
tuned_test_predictions = apply_threshold(tuned_test_raw, tuned_threshold)
tuned_metrics = evaluate_model(
    tuned_test_predictions,
    f"Random Forest [{best_strategy_name}] Tuned",
    tuned_threshold,
)
tuned_metrics["Imbalance Strategy"] = best_strategy_name
tuned_metrics["Model Variant"] = "Tuned RF"
tuned_metrics["RF Params"] = format_rf_params(tuned_rf_params)

baseline_best_metrics = next(r for r in rf_strategy_results if r["Imbalance Strategy"] == best_strategy_name)
baseline_summary = dict(baseline_best_metrics)
baseline_summary["Model Variant"] = "Baseline RF"
baseline_summary["RF Params"] = f"numTrees={STRATEGY_NUM_TREES}, maxDepth=10"

tuned_auc_pr = float(tuned_metrics["AUC-PR"])
tuned_f1 = float(tuned_metrics["F1 Score"])
baseline_auc_pr = float(baseline_best_metrics["AUC-PR"])
baseline_f1 = float(baseline_best_metrics["F1 Score"])

use_tuned_model = (tuned_auc_pr > baseline_auc_pr) or (
    abs(tuned_auc_pr - baseline_auc_pr) < 1e-12 and tuned_f1 >= baseline_f1
)

final_rf_compare_pd = pd.DataFrame([baseline_summary, tuned_metrics]).sort_values(
    by=["AUC-PR", "F1 Score"],
    ascending=False,
)
print("\n=== BASELINE VS TUNED RF (BEST STRATEGY) ===")
print(final_rf_compare_pd.to_string(index=False))

if use_tuned_model:
    selected_rf_model_for_importance = tuned_rf_model
    selected_strategy_label = f"{best_strategy_name} + Tuned RF"
    selected_final_threshold = float(tuned_threshold)
    selected_final_raw_predictions = tuned_test_raw
    print("Using tuned RF model for final feature importance and reporting.")
else:
    selected_rf_model_for_importance = rf_strategy_models.get(best_strategy_name, rf_model)
    selected_strategy_label = best_strategy_name
    selected_final_threshold = float(baseline_best_metrics["Threshold"])
    selected_final_raw_predictions = selected_rf_model_for_importance.transform(test_data)
    print("Tuned RF did not outperform baseline by AUC-PR/F1. Keeping baseline best-strategy RF.")


def compute_precision_lift_at_k(raw_predictions, model_name, k_levels=(0.01, 0.03, 0.05)):
    scored = raw_predictions.withColumn("score", vector_to_array(F.col("probability")).getItem(1)).select(
        "label", "score"
    )
    total_rows = scored.count()
    if total_rows == 0:
        print(f"No rows found for Precision@K computation on {model_name}.")
        return pd.DataFrame()

    base_rate = float(scored.agg(F.avg("label").alias("base_rate")).first()["base_rate"] or 0.0)
    rows = []

    for k in k_levels:
        quantile = max(0.0, min(1.0, 1.0 - float(k)))
        score_threshold = float(scored.approxQuantile("score", [quantile], 0.001)[0])
        topk = scored.filter(F.col("score") >= F.lit(score_threshold))
        topk_count = int(topk.count())
        positives = float(topk.agg(F.sum("label").alias("positives")).first()["positives"] or 0.0)

        precision_k = (positives / topk_count) if topk_count else 0.0
        lift_k = (precision_k / base_rate) if base_rate else 0.0

        rows.append(
            {
                "k": k,
                "threshold": round(score_threshold, 4),
                "selected_rows": topk_count,
                "selected_share": round(topk_count / total_rows, 4),
                "precision_at_k": round(precision_k, 4),
                "lift_at_k": round(lift_k, 4),
            }
        )

    topk_pd = pd.DataFrame(rows)
    print(f"\n=== Precision@K and Lift@K [{model_name}] ===")
    print(f"Baseline positive rate (random precision): {base_rate:.4f}")
    print(topk_pd.to_string(index=False))
    return topk_pd


print(f"\nFinal selected threshold for {selected_strategy_label}: {selected_final_threshold:.2f}")
_ = compute_precision_lift_at_k(selected_final_raw_predictions, selected_strategy_label)

# COMMAND ----------

importances = selected_rf_model_for_importance.featureImportances.toArray()
feature_importance_list = list(zip(feature_columns, importances))
feature_importance_list.sort(key=lambda x: x[1], reverse=True)

print(f"\n=== Feature Importance (Random Forest: {selected_strategy_label}) ===")
for feat, imp in feature_importance_list:
    print(f"{feat:30s}: {imp:.4f}")

top3_features = feature_importance_list[:3]
print("\nTop 3 features for purchase prediction:")
for i, (feat, imp) in enumerate(top3_features, start=1):
    print(f"{i}. {feat} ({imp:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC **Model Selection Rationale (P5-T2)**
# MAGIC We trained Logistic Regression, Decision Tree, and Random Forest with class-weighted fitting, then tuned decision thresholds on a validation split before final test evaluation.
# MAGIC Threshold selection now follows a precision-first policy with a minimum recall floor, which is more aligned with minority-class targeting than F1-only thresholding.
# MAGIC We also compared Random Forest across multiple rebalancing strategies (class weighting baseline, manual undersampling, and optional manual oversampling/SMOTE-like synthetic generation depending on run profile) to satisfy explicit imbalance-handling checks.
# MAGIC After selecting the best imbalance strategy, we ran targeted Random Forest hyperparameter tuning (trees/depth/node constraints/subsampling) and kept the tuned model only when it improved AUC-PR/F1 over baseline.
# MAGIC Because purchase sessions are a minority class (~3.44%), accuracy alone can be misleading; positive-class Precision/Recall/F1 and AUC-PR are stronger decision metrics for this task.
# MAGIC Precision@K and Lift@K are also reported for the selected final model to connect minority-class ranking quality to business targeting value.
# MAGIC Select the final strategy that best balances minority-class detection quality (F1), ranking quality for rare events (AUC-PR), and stable general discrimination (AUC-ROC), then use feature importance for business interpretation.

# COMMAND ----------

# MAGIC %md
# MAGIC ### P5-T2 Checkpoint Summary
# MAGIC - Logistic Regression, Decision Tree, and Random Forest were trained with class-weighted fitting on the same split.
# MAGIC - Validation-based threshold tuning now uses a precision-at-recall policy (recall floor + precision-first selection).
# MAGIC - Additional imbalance experiments were added for Random Forest with profile-based execution (free-tier vs full) to remain reproducible under runtime limits.
# MAGIC - Targeted hyperparameter tuning was run on the best imbalance strategy and compared against its baseline RF configuration.
# MAGIC - A consistent imbalance-aware comparison table (Accuracy, Precision, Recall, F1, AUC-ROC, AUC-PR, threshold) was generated for model selection.
# MAGIC - Precision@K and Lift@K were computed for the selected final RF model to quantify business targeting quality under class imbalance.
# MAGIC - Random Forest feature importance was extracted and ranked.
# MAGIC - Selection guidance explicitly accounts for class imbalance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### P5-T2c: Advanced Evaluation Notebook (Recommended)
# MAGIC
# MAGIC To keep this notebook focused and avoid overloading Phase 5 cells, advanced evaluation is provided in a separate notebook:
# MAGIC - `src/P5_Advanced_Evaluation.py`
# MAGIC
# MAGIC That notebook adds:
# MAGIC 1. Time-based validation split (chronological train/validation/test)
# MAGIC 2. Probability calibration (Platt scaling)
# MAGIC 3. Cost-based threshold optimization
# MAGIC 4. Final calibrated test comparison + Precision@K/Lift@K
# MAGIC 5. Precision-Recall curves and capacity-aware Top-K targeting
# MAGIC
# MAGIC Optional heavy boosting experiments are isolated in a separate notebook-script:
# MAGIC - `src/P5_Boosting_Experiments.py`
# MAGIC   (XGBoost/LightGBM attempts run only there to avoid heavy tasks in main notebooks)
# MAGIC
# MAGIC Recommended execution order:
# MAGIC 1. Run this notebook through P5-T2 / P5-T2b
# MAGIC 2. Open and run `src/P5_Advanced_Evaluation.py` for advanced evaluation outputs
# MAGIC 3. Run `src/P5_Boosting_Experiments.py` only if boosting dependencies are available and quota allows

# COMMAND ----------

# MAGIC %md
# MAGIC ### P5-T3 (Additional Analysis): K-Means Customer Segmentation (Bonus)
# MAGIC
# MAGIC This optional bonus analysis applies unsupervised clustering to user-level behavior profiles.
# MAGIC It complements the supervised purchase-prediction models with customer segment discovery.
# MAGIC
# MAGIC Workflow:
# MAGIC - Build user-level features from cleaned events
# MAGIC - Scale features with `StandardScaler(withMean=False)` for Spark vector compatibility
# MAGIC - Train K-Means (`k=3`) and evaluate with silhouette score
# MAGIC - Assign business labels to clusters based on spend profile

# COMMAND ----------

from pyspark.ml.clustering import KMeans  # noqa: E402
from pyspark.ml.evaluation import ClusteringEvaluator  # noqa: E402
from pyspark.ml.feature import StandardScaler, VectorAssembler  # noqa: E402
from pyspark.ml.functions import vector_to_array as cluster_vector_to_array  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

if "df_clean" not in globals():
    raise ValueError("df_clean is not available. Run Step 3 before P5-T3.")

required_cluster_cols = ["user_id", "user_session", "event_type", "price", "brand"]
missing_cluster_cols = [c for c in required_cluster_cols if c not in df_clean.columns]
if missing_cluster_cols:
    raise ValueError(f"df_clean is missing required columns for P5-T3: {missing_cluster_cols}")

user_features = (
    df_clean.filter(F.col("user_id").isNotNull())
    .groupBy("user_id")
    .agg(
        F.count("*").alias("total_events"),
        F.count(F.when(F.col("event_type") == "purchase", True)).alias("purchase_count"),
        F.count(F.when(F.col("event_type") == "view", True)).alias("view_count"),
        F.countDistinct("user_session").alias("num_sessions"),
        F.avg("price").alias("avg_price"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("total_spend"),
        F.countDistinct("brand").alias("unique_brands"),
    )
)

# Stabilize heavy-tailed behavioral counts before clustering to reduce outlier-dominated micro-clusters.
heavy_tail_cols = ["total_events", "purchase_count", "view_count", "num_sessions", "total_spend"]
clip_quantile = 0.995
clip_values = {c: user_features.approxQuantile(c, [clip_quantile], 0.001)[0] for c in heavy_tail_cols}

for c in heavy_tail_cols:
    user_features = user_features.withColumn(
        f"{c}_capped",
        F.least(F.col(c).cast("double"), F.lit(float(clip_values[c]))),
    )

user_model_data = (
    user_features.withColumn("avg_price", F.col("avg_price").cast("double"))
    .withColumn("unique_brands", F.col("unique_brands").cast("double"))
    .withColumn("log_total_events", F.log1p(F.col("total_events_capped")))
    .withColumn("log_purchase_count", F.log1p(F.col("purchase_count_capped")))
    .withColumn("log_view_count", F.log1p(F.col("view_count_capped")))
    .withColumn("log_num_sessions", F.log1p(F.col("num_sessions_capped")))
    .withColumn("log_total_spend", F.log1p(F.col("total_spend_capped")))
)

cluster_feature_cols = [
    "log_total_events",
    "log_purchase_count",
    "log_view_count",
    "log_num_sessions",
    "avg_price",
    "log_total_spend",
    "unique_brands",
]

assembler = VectorAssembler(inputCols=cluster_feature_cols, outputCol="raw_features", handleInvalid="skip")
user_data = assembler.transform(user_model_data).select(
    "user_id",
    "total_events",
    "purchase_count",
    "view_count",
    "num_sessions",
    "avg_price",
    "total_spend",
    "unique_brands",
    "raw_features",
)

scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)
scaler_model = scaler.fit(user_data)
user_data_scaled = scaler_model.transform(user_data)

kmeans = KMeans(featuresCol="features", predictionCol="prediction", k=3, seed=42)
km_model = kmeans.fit(user_data_scaled)
km_predictions = km_model.transform(user_data_scaled)

cluster_evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction", metricName="silhouette")
silhouette = cluster_evaluator.evaluate(km_predictions)

cluster_centers = [center.tolist() for center in km_model.clusterCenters()]
feature_dimension = len(cluster_centers[0])

km_with_distances = km_predictions.withColumn("features_arr", cluster_vector_to_array(F.col("features")))

distance_expr = None
for cluster_id, center in enumerate(cluster_centers):
    squared_sum_expr = F.lit(0.0)
    for idx in range(feature_dimension):
        squared_sum_expr = squared_sum_expr + F.pow(
            F.col("features_arr").getItem(idx) - F.lit(float(center[idx])),
            2,
        )

    current_distance_expr = F.sqrt(squared_sum_expr)
    if distance_expr is None:
        distance_expr = F.when(F.col("prediction") == F.lit(cluster_id), current_distance_expr)
    else:
        distance_expr = distance_expr.when(F.col("prediction") == F.lit(cluster_id), current_distance_expr)

distance_expr = distance_expr.otherwise(F.lit(None).cast("double"))

km_with_distances = km_with_distances.withColumn("dist_to_center", distance_expr)

cluster_spread = (
    km_with_distances.groupBy("prediction")
    .agg(F.round(F.avg("dist_to_center"), 6).alias("avg_intra_dist"))
    .orderBy("prediction")
)

spread_rows = cluster_spread.collect()
spread_map = {int(r["prediction"]): float(r["avg_intra_dist"] or 0.0) for r in spread_rows}

db_components = []
for i in range(len(cluster_centers)):
    s_i = spread_map.get(i, 0.0)
    r_values = []

    for j in range(len(cluster_centers)):
        if i == j:
            continue

        s_j = spread_map.get(j, 0.0)
        m_ij = (sum((float(a) - float(b)) ** 2 for a, b in zip(cluster_centers[i], cluster_centers[j]))) ** 0.5

        if m_ij > 0:
            r_values.append((s_i + s_j) / m_ij)

    db_components.append(max(r_values) if r_values else 0.0)

davies_bouldin = sum(db_components) / len(db_components)

cluster_summary = km_predictions.groupBy("prediction").agg(
    F.count("*").alias("num_customers"),
    F.round(F.avg("total_events"), 2).alias("avg_events"),
    F.round(F.avg("purchase_count"), 2).alias("avg_purchases"),
    F.round(F.avg("view_count"), 2).alias("avg_views"),
    F.round(F.avg("num_sessions"), 2).alias("avg_sessions"),
    F.round(F.avg("avg_price"), 2).alias("avg_price"),
    F.round(F.avg("total_spend"), 2).alias("avg_spend"),
    F.round(F.sum("total_spend"), 2).alias("cluster_total_spend"),
)

totals_row = cluster_summary.agg(
    F.sum("num_customers").alias("total_customers"),
    F.sum("cluster_total_spend").alias("total_spend_all"),
).first()

total_customers_all = int(totals_row["total_customers"] or 0)
total_spend_all = float(totals_row["total_spend_all"] or 0.0)

spend_rank_window = Window.partitionBy(F.lit(1)).orderBy(F.col("avg_spend").desc())

cluster_summary_labeled = (
    cluster_summary.withColumn(
        "customer_share_pct",
        F.round((F.col("num_customers") / F.lit(total_customers_all)) * 100, 2),
    )
    .withColumn(
        "spend_share_pct",
        F.round((F.col("cluster_total_spend") / F.lit(total_spend_all)) * 100, 2),
    )
    .withColumn("spend_rank", F.row_number().over(spend_rank_window))
    .withColumn(
        "segment_label",
        F.when(F.col("spend_rank") == 1, F.lit("High-Value Customers"))
        .when(F.col("spend_rank") == 2, F.lit("Regular Customers"))
        .otherwise(F.lit("Casual Browsers")),
    )
    .select(
        "prediction",
        "segment_label",
        "num_customers",
        "customer_share_pct",
        "avg_events",
        "avg_purchases",
        "avg_views",
        "avg_sessions",
        "avg_price",
        "avg_spend",
        "cluster_total_spend",
        "spend_share_pct",
    )
    .orderBy("prediction")
)

print(f"Silhouette Score (k=3): {silhouette:.4f}")
print(f"Davies-Bouldin Index (k=3): {davies_bouldin:.4f}")
print("Interpretation: lower Davies-Bouldin values indicate tighter and better-separated clusters.")
print("\n=== Cluster Intra-Distance Summary ===")
cluster_spread.show(truncate=False)
print("\n=== Cluster Summary with Business Labels ===")
cluster_summary_labeled.show(truncate=False)

cluster_balance = cluster_summary_labeled.agg(
    F.min("customer_share_pct").alias("min_cluster_share_pct"),
    F.max("customer_share_pct").alias("max_cluster_share_pct"),
).first()

print(
    "Cluster size spread (share %): "
    f"min={cluster_balance['min_cluster_share_pct']:.2f}, "
    f"max={cluster_balance['max_cluster_share_pct']:.2f}"
)

high_value_segment = (
    cluster_summary_labeled.filter(F.col("segment_label") == "High-Value Customers")
    .select("customer_share_pct", "spend_share_pct")
    .first()
)

print(
    "\nActionable insight: High-Value Customers represent "
    f"{high_value_segment['customer_share_pct']:.2f}% of users and "
    f"{high_value_segment['spend_share_pct']:.2f}% of total spend."
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretation — P5-T3 (Bonus Segmentation)**
# MAGIC The silhouette score quantifies segment separation quality (higher is better), while the Davies-Bouldin index captures compactness vs separation (lower is better).
# MAGIC Cluster labels are assigned by spend profile so results can be mapped to actionable customer tiers:
# MAGIC - High-Value Customers
# MAGIC - Regular Customers
# MAGIC - Casual Browsers
# MAGIC
# MAGIC Use customer-share and spend-share together to prioritize retention, upsell, and campaign budget allocation.

# COMMAND ----------

# MAGIC %md
# MAGIC ### P5-T3 Checkpoint Summary
# MAGIC - K-Means clustering (`k=3`) trained successfully on standardized user-level behavior features.
# MAGIC - Silhouette score and Davies-Bouldin index are computed and printed for clustering quality assessment.
# MAGIC - Cluster summary includes clear differences in events, purchases, sessions, and spend.
# MAGIC - Business segment labels were assigned to each cluster based on spend ranking.
# MAGIC - This section is explicitly marked as Additional Analysis (Bonus).
