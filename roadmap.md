# PUSL3121 Big Data Analytics — Full Project Roadmap

**Project:** Industry-Based Big Data Analytics Using Apache Spark
**Industry:** E-Commerce (Cosmetics Online Store)
**Dataset:** eCommerce Events History in Cosmetics Shop (Kaggle — REES46)
**Deadline:** April 9, 2026, 16:00 SL Time
**Team Size:** 4 Members
**Target Grade:** 80%+

### Development Environment

| Component | Detail |
| --- | --- |
| Databricks | 14-Day Premium Free Trial (`dbc-1481bae3-1700.cloud.databricks.com`) |
| Compute | Databricks Serverless (no manual cluster management) |
| IDE | VS Code + Databricks Extension + Databricks Connect 15.1.0 |
| AI Agents | GitHub Copilot / Claude Code (local, synced to Databricks) |
| Code Format | Python files with `# COMMAND ----------` cell markers in `src/` folder |
| Trial Expiry | ~April 11, 2026 (2 days buffer after deadline) |

---

## PROJECT OVERVIEW

### Dataset Identity

| Field | Detail |
| --- | --- |
| Name | eCommerce Events History in Cosmetics Shop |
| Source | https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop |
| Author | Michael Kechinov / REES46 Marketing Platform |
| Size | ~20 million events, 5 CSV files (~2GB+ combined) |
| Time Range | October 2019 – February 2020 (5 months) |
| Files | `2019-Oct.csv`, `2019-Nov.csv`, `2019-Dec.csv`, `2020-Jan.csv`, `2020-Feb.csv` |

### Dataset Columns (9 fields)

| Column | Type | Description |
| --- | --- | --- |
| `event_time` | timestamp | When the event happened (UTC) |
| `event_type` | string | Type of event: `view`, `cart`, `purchase`, `remove_from_cart` |
| `product_id` | long | Unique product identifier |
| `category_id` | long | Product category ID |
| `category_code` | string | Category taxonomy path (e.g., `cosmetics.body.body_care`). Can be null |
| `brand` | string | Brand name (lowercase). Can be null |
| `price` | float | Product price in USD |
| `user_id` | long | Permanent user identifier |
| `user_session` | string | Temporary session ID (changes per visit) |

### Business Problem

**"Can we predict whether a customer browsing a cosmetics e-commerce website will make a purchase, and what factors drive purchasing behavior?"**

This maps to:

- **Descriptive Analytics:** What happened? → Purchase patterns, popular products, revenue trends
- **Predictive Analytics:** What will happen? → Binary classification — will a user-session lead to purchase?
- **Prescriptive Analytics:** What should we do? → Business recommendations based on insights

### Mark Allocation Map

| Step | What | Marks | Priority |
| --- | --- | --- | --- |
| Step 1 | Dataset Selection & Industry Context | 10 | Medium |
| Step 2 | NoSQL Storage (MongoDB) | 10 | Medium |
| Step 3 | Data Processing with Spark | 20 | **HIGH** |
| Step 4 | Data Visualization | 10 | Medium |
| Step 5 | Predictive Analytics (Spark ML) | 15 | **HIGH** |
| Step 6 | Big Data Architecture Diagram | 10 | Medium |
| — | Report quality, code quality, video | ~25 | **HIGH** |
| **Total** |  | **~100** |  |

### Deliverables Checklist

- [ ]  Databricks Notebook (exported `.dbc` or `.html` + sharable link)
- [ ]  Written Report (~2500 words, Harvard referencing)
- [ ]  Video Presentation (20–30 min Zoom recording, all 4 members)
- [ ]  All resources linked/attached in DLE submission

### Team Task Allocation

| Member | Owns (Report + Video) | Report Words | Video Time |
| --- | --- | --- | --- |
| **You**  | Step 3 (Spark Processing) + Step 5 (ML) | ~700 words | ~8 min |
| **Member 2** | Step 1 (Dataset & Context) + Step 2 (MongoDB) | ~600 words | ~7 min |
| **Member 3** | Step 4 (Visualization) + Step 6 (Architecture) | ~600 words | ~7 min |
| **Member 4** | Introduction + Conclusion + Business Implications + References | ~600 words | ~7 min |

### Timeline Overview (12 Days — Starting Mar 28)

| Day | Date | Phase | Focus |
| --- | --- | --- | --- |
| Day 1 | Mar 28 (TODAY) | Phase 0 + 1 | Finish setup + extract dataset + upload + load & explore |
| Day 2 | Mar 29 | Phase 2 | MongoDB Atlas setup + sample storage |
| Day 3–4 | Mar 30–31 | Phase 3 | Spark data processing (heaviest phase) |
| Day 5 | Apr 1 | Phase 4 | Visualizations |
| Day 6–7 | Apr 2–3 | Phase 5 | Machine learning models |
| Day 8 | Apr 4 | Phase 6 | Architecture diagram |
| Day 8–9 | Apr 4–5 | Phase 7 | Report writing (all members) |
| Day 10 | Apr 6 | Phase 8 | Video recording |
| Day 11 | Apr 7 | Phase 9 | Final review + buffer |
| Day 12 | Apr 8 | — | Emergency buffer — submit by Apr 9 |

**⚠️ Trial expiry:** Premium trial expires ~April 11. Submit by Apr 8 evening. Apr 9 is emergency-only.
**⚠️ Export before trial ends:** Export all notebooks on Apr 8 at the latest — once trial expires you lose workspace access.

---

## PHASE 0: Prerequisites & Environment Setup

**Goal:** Finish environment wiring and get dataset uploaded. Most infrastructure is already in place.

**Time Estimate:** 1–2 hours (most setup already done)

**⚠️ DO NOT SKIP THIS PHASE.** Everything downstream depends on data being accessible.

---

### P0-T1: Verify Databricks Premium Trial + VS Code Connection

**Who:** You (Lead)

**Status: ALREADY DONE** — Your screenshot confirms:

- Databricks Premium Trial workspace active at `dbc-1481bae3-1700.cloud.databricks.com`
- VS Code Databricks Extension connected (Databricks Connect 15.1.0)
- Serverless compute configured
- Sync state: `WATCHING_FOR_CHANGES` (live sync working)

**Verify these work:**

1. In VS Code, create a test file `src/test.py`:

```python
# COMMAND ----------
print("Hello from Databricks Serverless!")

# COMMAND ----------
print(f"Spark version: {spark.version}")
```

1. Run each cell using the Databricks extension (click the run button next to each cell or use the keyboard shortcut)
2. Confirm output appears in the VS Code output panel

**⚠️ SERVERLESS DIFFERENCES FROM COMMUNITY EDITION:**

- No cluster to create or manage — compute starts automatically when you run code
- No 2-hour idle timeout — serverless scales down automatically and restarts instantly
- Better performance — serverless has more resources than CE's single node
- Unity Catalog is likely enabled — affects file storage paths (see P0-T3)
- `dbutils` works via Databricks Connect but some commands may be limited

### ✅ Gate Check P0-T1:

- [x]  VS Code → Databricks sync is active (WATCHING_FOR_CHANGES)
- [x]  Can run a Python cell and see output
- [x]  `spark.version` prints a version string (e.g., 3.5.x)

---

### P0-T2: Extract & Upload Dataset to Databricks

**Who:** You (Lead)

**Step A — Extract the CSVs (you have the ZIPs already):**

1. You have 5 `.csv.zip` files (~440MB total compressed)
2. Extract each one — right-click → Extract All (Windows)
3. You should get 5 `.csv` files totaling **~2.4 GB**
4. Verify: `2019-Oct.csv`, `2019-Nov.csv`, `2019-Dec.csv`, `2020-Jan.csv`, `2020-Feb.csv`

**Step B — Upload to Databricks (choose ONE method):**

**METHOD 1: DBFS via Databricks UI (SIMPLEST — recommended)**

1. Go to your Databricks workspace in browser: `https://dbc-1481bae3-1700.cloud.databricks.com`
2. Left sidebar → **Catalog** → **Browse DBFS** (or go to **Data** → **DBFS**)
3. Navigate to `/FileStore/tables/` (create the `tables` folder if it doesn't exist)
4. Upload each CSV file one at a time (Premium trial has higher upload limits than CE)
5. Verify in VS Code:

```python
# COMMAND ----------
display(dbutils.fs.ls("/FileStore/tables/"))
```

**METHOD 2: Unity Catalog Volumes (MODERN — premium-only feature)**

1. In the Databricks UI: **Catalog** → select your catalog (usually `main`) → select a schema (e.g., `default`)
2. Click **Create Volume** → Name: `cosmetics_data` → Type: Managed
3. Upload CSVs to the volume
4. Your file path becomes: `/Volumes/workspace/default/cosmetics_data/2019-Oct.csv`
5. Verify:

```python
# COMMAND ----------
display(dbutils.fs.ls("/Volumes/workspace/default/cosmetics_data/"))
```

**⚠️ IMPORTANT — Set DATA_PATH based on your upload method:**

```python
# Current project default (this repository uses Volumes):
DATA_PATH = "/Volumes/workspace/default/cosmetics_data"

# If you used DBFS (Method 1), switch to:
# DATA_PATH = "/FileStore/tables"
```

All code in the roadmap uses `DATA_PATH` — set it once at the top and everything else works regardless of which method you chose.

**⚠️ WHICH METHOD TO CHOOSE:**

- **DBFS** if you want zero friction and guaranteed compatibility with all existing code
- **Unity Catalog Volumes** if DBFS browsing isn't available in your workspace UI (some Premium trials have it disabled)
- Either way, the PySpark code is identical — only the path string changes

### ✅ Gate Check P0-T2:

- [x]  All 5 CSV files extracted (total ~2.4 GB)
- [x]  All 5 CSVs uploaded to Databricks (DBFS or Volume)
- [x]  `dbutils.fs.ls(DATA_PATH)` shows all 5 files from VS Code
- [x]  File sizes are in the hundreds-of-MB range (not KB — that means you uploaded ZIPs instead of CSVs)

---

### P0-T3: Create MongoDB Atlas Account & Cluster

**Who:** You (Lead) — OR delegate to Member 2 since they own Step 2

**Steps:**

1. Go to https://www.mongodb.com/cloud/atlas/register
2. Sign up with university email
3. Choose **FREE Shared Cluster** (M0 tier)
4. Settings:
    - **Cloud Provider:** AWS (default is fine)
    - **Region:** Pick closest to your location
    - **Cluster Name:** `PUSL3121-Cosmetics`
5. Click **Create Cluster** (takes 1–3 min)
6. Set up **Database Access:**
    - Go to Security > Database Access
    - Add new user: username `admin`, password (note it down)
    - Role: Atlas Admin
7. Set up **Network Access:**
    - Go to Security > Network Access
    - Click Add IP Address > **Allow Access from Anywhere** (0.0.0.0/0)
    - This is fine for a coursework project
8. Get **Connection String:**
    - Go to Clusters > Connect > Drivers
    - Copy the connection string (looks like `mongodb+srv://admin:<password>@pusl3121-cosmetics.xxxxx.mongodb.net/`)

**Expected Output:**

- MongoDB Atlas dashboard shows cluster `PUSL3121-Cosmetics` in status "Active"
- You have a connection string ready
- Database user credentials noted down

**⚠️ IMPORTANT:** Free tier limit is **512MB storage**. This is fine — Step 2 only asks for "sample records" (we'll store ~5,000–10,000 documents, not the full dataset).

### ✅ Gate Check P0-T3:

- [x]  Atlas cluster is active and running
- [x]  Database user created with credentials noted
- [x]  Network access set to allow connections
- [x]  Connection string copied and saved

---

### P0-T4: Create Project File Structure in VS Code

**Who:** You (Lead)

**Your VS Code project should have this structure:**

```
databricks-workspace/          ← your existing project root
├── databricks.yml                    ← already exists (Databricks config)
├── src/
│   ├── Main_Analysis.py               ← PRIMARY notebook (Steps 1,3,4,5,6)
│   ├── MongoDB_Demo.py                ← Step 2 standalone
│   └── ...                           ← other existing files (ignore them)
├── .vscode/
│   └── settings.json                 ← already exists
└── ...
```

**Step 1 — Create `src/Main_Analysis.py`:**

```python
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
# MAGIC **Note:** MongoDB/NoSQL demonstration is in the companion notebook: MongoDB_Demo

# COMMAND ----------

# Configuration — set this once based on your upload method
DATA_PATH = "/Volumes/workspace/default/cosmetics_data"  # Switch to "/FileStore/tables" only if using DBFS

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import *
```

**Step 2 — Create `src/MongoDB_Demo.py`:**

```python
# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # PUSL3121 - MongoDB/NoSQL Demonstration (Step 2)
# MAGIC ## Storing E-Commerce Event Data in MongoDB Atlas

# COMMAND ----------

# MAGIC %pip install "pymongo[srv]"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

DATA_PATH = "/Volumes/workspace/default/cosmetics_data"  # Match your main notebook
```

**Step 3 — Verify sync:**

- Save both files
- Check the VS Code terminal — you should see `Uploaded src/Main_Analysis.py` and `Uploaded src/MongoDB_Demo.py`
- These files now appear as notebooks in your Databricks workspace

**⚠️ KEY VS CODE WORKFLOW NOTES:**

| Concept | How it works |
| --- | --- |
| **Cell separator** | `# COMMAND ----------` between code blocks |
| **Markdown cells** | Start with `# MAGIC %md` then `# MAGIC` before each line |
| **Running cells** | Click the play button next to each `# COMMAND` or use Ctrl+Shift+Enter |
| **Output** | Appears in VS Code's output panel (bottom of screen) |
| **Sync** | Automatic — save file → syncs to Databricks workspace instantly |
| **pip install** | Use `# MAGIC %pip install package` in a cell (runs on serverless) |
| **display()** | Works — output shows in VS Code's Databricks output panel |
| **plt.show()** | Matplotlib charts render in the Databricks output panel |

**⚠️ CRITICAL — `# MAGIC` prefix:**

- Markdown cells: every line must start with `# MAGIC`
- Magic commands (`%pip`, `%md`, `%sql`): prefix with `# MAGIC`
- Regular Python code: NO prefix — just write normal Python

### ✅ Gate Check P0-T4:

- [x]  Both `.py` files created in `src/` folder
- [x]  Both files synced to Databricks workspace (check terminal for upload messages)
- [x]  Can run the first cell of Main_Analysis and see output
- [x]  `DATA_PATH` variable is set correctly in both files

---

## PHASE 0 — COMPLETE VERIFICATION

Before proceeding to Phase 1, verify ALL of the following:

- [x]  Databricks Premium Trial workspace accessible
- [x]  VS Code connected to Databricks (sync active)
- [x]  Serverless compute runs code successfully
- [x]  All 5 extracted CSV files uploaded to Databricks (DBFS or Volumes)
- [x]  `dbutils.fs.ls(DATA_PATH)` shows all 5 CSVs with correct sizes (~hundreds of MB each)
- [x]  MongoDB Atlas cluster active with credentials
- [x]  Both Python files created and synced to workspace
- [x]  Can run Python cells from VS Code and see output

**If any item fails:** Fix it before moving on. Do not proceed with broken infrastructure.

---

## PHASE 1: Dataset Exploration & Understanding (Feeds Step 1 — 10 Marks)

**Goal:** Load the dataset, understand its structure, and document the industry context and business problem.

**Time Estimate:** 2–3 hours

**Notebook:** `src/Main_Analysis.py` (Section 1)

**⚠️ VS CODE WORKFLOW:** Add code below the setup cells you created in Phase 0. Each new block of code should be separated by `# COMMAND ----------`. Run cells individually using the Databricks extension play button.

---

### P1-T1: Load and Inspect the Full Dataset

**Who:** You (Lead)

### 🤖 Agent Prompt for P1-T1:

```
TASK: Load and explore the eCommerce Events History in Cosmetics Shop dataset.

CONTEXT: We have 5 CSV files uploaded to Databricks Volumes at /Volumes/workspace/default/cosmetics_data/:
- 2019-Oct.csv, 2019-Nov.csv, 2019-Dec.csv, 2020-Jan.csv, 2020-Feb.csv

Columns: event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session

⚠️ TIMESTAMP FORMAT: The dataset uses "2019-10-01 00:00:00 UTC" format.
inferSchema may NOT parse the UTC suffix correctly. You must handle this.

WORKFLOW:
1. Load all 5 CSV files into a single Spark DataFrame:

   from functools import reduce
   from pyspark.sql import functions as F
   from pyspark.sql.types import *

   # Define path variable at the top (makes switching between local/Databricks easy)
   DATA_PATH = "/Volumes/workspace/default/cosmetics_data"

   months = ["2019-Oct", "2019-Nov", "2019-Dec", "2020-Jan", "2020-Feb"]
   dfs = []
   for m in months:
       try:
           temp = spark.read.csv(f"{DATA_PATH}/{m}.csv", header=True, inferSchema=True)
           dfs.append(temp)
           print(f"Loaded {m}: {temp.count()} rows")
       except Exception as e:
           print(f"Could not load {m}: {e}")

   df = reduce(lambda a, b: a.union(b), dfs)
   print(f"\\nTotal rows: {df.count()}")

2. Fix timestamp parsing (CRITICAL):
   # Check if event_time parsed as timestamp or string
   df.printSchema()

   # If event_time is a string, convert it:
   # The dataset format is "2019-10-01 00:00:00 UTC" — strip "UTC" and parse
   if str(df.schema["event_time"].dataType) == "StringType":
       df = df.withColumn("event_time",
           F.to_timestamp(F.regexp_replace(F.col("event_time"), " UTC", ""),
                          "yyyy-MM-dd HH:mm:ss"))
       print("Converted event_time from string to timestamp")

   # Verify it worked
   df.select("event_time").show(5)

3. Basic exploration:
   - df.printSchema() — verify column types
   - df.count() — total number of rows
   - df.show(10) — sample rows
   - df.describe().show() — summary statistics
   - df.select("event_type").distinct().show() — unique event types

4. Count events by type:
   df.groupBy("event_type").count().orderBy("count", ascending=False).show()

   Expected output: view (majority ~85-90%), cart (~5-8%), purchase (~2-5%), remove_from_cart (~2-3%)

5. Check for nulls:
   null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
   null_counts.show()

   Expected: category_code and brand will have significant nulls. Others should be mostly complete.

6. Add a comment/markdown cell summarizing findings

ARTIFACTS:
- All code cells with outputs visible
- Markdown summary cell at the end
- DATA_PATH variable defined at top for portability

EXPECTED OUTCOME:
- Total rows: approximately 15-20 million
- 4 event types: view, cart, purchase, remove_from_cart
- event_time correctly parsed as TimestampType
- Nulls mainly in category_code and brand
- Price range: $0 to several hundred USD
- User IDs in the hundreds of thousands range
```

**Expected Output:**

```
Schema:
root
 |-- event_time: timestamp (nullable = true)
 |-- event_type: string (nullable = true)
 |-- product_id: long (nullable = true)
 |-- category_id: long (nullable = true)
 |-- category_code: string (nullable = true)
 |-- brand: string (nullable = true)
 |-- price: double (nullable = true)
 |-- user_id: long (nullable = true)
 |-- user_session: string (nullable = true)

Event type counts (approximate):
+------------------+----------+
|        event_type|     count|
+------------------+----------+
|              view|  15000000|
|              cart|   1200000|
|          purchase|    700000|
|remove_from_cart  |    600000|
+------------------+----------+

Null counts:
- category_code: ~98.29% null (observed in current project run)
- brand: ~42.32% null (observed in current project run)
- Others: minimal or zero nulls
```

### ✅ Gate Check P1-T1:

- [x]  DataFrame loads without errors
- [x]  `df.count()` returns millions of rows (15M+)
- [x]  Schema shows correct column types (timestamp for event_time, string for event_type, etc.)
- [x]  4 distinct event types visible
- [x]  Null analysis complete — you know exactly which columns have missing data

---

### P1-T2: Document Industry Context and Business Problem

**Who:** This feeds into Member 2's report section, but you should draft the key points now.

**What to document (as markdown cells in notebook OR as notes):**

```
INDUSTRY CONTEXT — E-Commerce (Cosmetics/Beauty Retail)

The global cosmetics e-commerce industry:
- Multi-billion dollar market growing rapidly
- High competition with low switching costs for customers
- Customer acquisition costs are high → retention is critical
- Understanding browsing behavior is key to improving conversion rates

BUSINESS PROBLEM:
"Predicting customer purchase conversion in an online cosmetics store"

Primary Question: Given a user's browsing session (views, cart additions),
can we predict whether they will complete a purchase?

Secondary Questions:
- What are the most popular product categories and brands?
- What time patterns exist in customer purchasing behavior?
- Can we segment customers into meaningful groups based on behavior?

HOW DATA ANALYTICS HELPS:
- Descriptive: Understand what happened — purchase patterns, popular products, revenue trends
- Predictive: Classify sessions likely to convert → targeted marketing
- Prescriptive: Recommend actions — optimal pricing, product placement, marketing timing

VALUE TO THE ORGANIZATION:
- Reduce cart abandonment (industry average ~70%)
- Improve marketing ROI through targeted campaigns
- Optimize product catalog based on demand patterns
- Increase customer lifetime value through personalization
```

### ✅ Gate Check P1-T2:

- [x]  Industry context is documented with real-world relevance
- [x]  Business problem is clearly defined as a question
- [x]  Connection between descriptive/predictive/prescriptive analytics is explicit
- [x]  At least 3 ways data analytics helps the organization are listed

---

## PHASE 1 — COMPLETE VERIFICATION

- [x]  Full dataset loaded into Spark DataFrame
- [x]  Row count, schema, event type distribution, and null analysis all documented
- [x]  Industry context and business problem clearly defined
- [x]  All code cells show visible outputs in the notebook

---

## PHASE 2: NoSQL Storage with MongoDB (Step 2 — 10 Marks)

**Goal:** Demonstrate storing sample records from the dataset in MongoDB Atlas and explain NoSQL concepts.

**Time Estimate:** 2–3 hours

**Notebook:** `src/MongoDB_Demo.py`

**⚠️ VS CODE WORKFLOW:** This is a separate file from the main analysis. Run it independently. After `%pip install` and `restartPython()`, you'll need to re-run the import cells — the VS Code extension handles this.

---

### P2-T1: Install PyMongo and Connect to MongoDB Atlas

**Who:** You (Lead) — or Member 2 if they're comfortable

### 🤖 Agent Prompt for P2-T1:

```
TASK: Connect to MongoDB Atlas from Databricks and store sample e-commerce records.

CONTEXT:
- MongoDB Atlas cluster is set up with connection string
- We need to store sample records from our cosmetics e-commerce dataset
- Free tier has 512MB limit — we'll store ~5,000-10,000 sample documents

⚠️ CRITICAL: Must install pymongo WITH the srv extra for Atlas connections.

WORKFLOW:
1. Install pymongo with SRV support (MUST use [srv] — plain pymongo won't connect to Atlas):
   # In VS Code, use MAGIC prefix for pip:
   # MAGIC %pip install "pymongo[srv]"
   # OR if running directly in the Databricks web UI notebook:
   %pip install "pymongo[srv]"

2. Restart Python after install (required in Databricks):
   dbutils.library.restartPython()

3. Load the dataset first (this notebook is standalone, doesn't share state with main notebook):
   from pyspark.sql import functions as F

   DATA_PATH = "/Volumes/workspace/default/cosmetics_data"
   df = spark.read.csv(f"{DATA_PATH}/2019-Oct.csv", header=True, inferSchema=True)
   print(f"Loaded {df.count()} rows for MongoDB demo")

4. Connect to MongoDB Atlas:
   from pymongo import MongoClient

   # Replace with your actual connection string
   connection_string = "mongodb+srv://admin:<password>@pusl3121-cosmetics.xxxxx.mongodb.net/"
   client = MongoClient(connection_string)

   # Create database and collection
   db = client["cosmetics_ecommerce"]
   collection = db["events"]

   # Test connection
   print("Connected successfully!")
   print("Databases:", client.list_database_names())

5. Prepare sample data from the Spark DataFrame:
   # Take a sample of 5000 purchase events (most interesting for analysis)
   sample_df = df.filter(df.event_type == "purchase").limit(5000)

   # Convert to list of dictionaries for MongoDB insertion
   sample_records = sample_df.toPandas().to_dict('records')

   # Convert timestamps to strings for JSON compatibility
   for record in sample_records:
       record['event_time'] = str(record['event_time'])

   print(f"Prepared {len(sample_records)} documents for insertion")

6. Insert documents into MongoDB:
   # Clear any existing data first
   collection.delete_many({})

   # Bulk insert
   result = collection.insert_many(sample_records)
   print(f"Inserted {len(result.inserted_ids)} documents")

7. Verify and query:
   # Count documents
   print(f"Total documents in collection: {collection.count_documents({})}")

   # Show one sample document
   import json
   sample = collection.find_one()
   print(json.dumps(sample, indent=2, default=str))

   # Query: Find purchases by a specific brand
   brand_purchases = list(collection.find({"brand": "runail"}).limit(5))
   print(f"\\nSample purchases for brand 'runail':")
   for doc in brand_purchases:
       print(json.dumps(doc, indent=2, default=str))

   # Aggregation: Average price by brand (top 10)
   pipeline = [
       {"$group": {"_id": "$brand", "avg_price": {"$avg": "$price"}, "count": {"$sum": 1}}},
       {"$sort": {"count": -1}},
       {"$limit": 10}
   ]
   results = list(collection.aggregate(pipeline))
   for r in results:
       print(f"Brand: {r['_id']}, Avg Price: ${r['avg_price']:.2f}, Count: {r['count']}")

ARTIFACTS:
- pymongo installed and connected
- 5000 documents inserted into MongoDB Atlas
- Sample document printed showing JSON structure
- At least one query and one aggregation demonstrated

EXPECTED DOCUMENT FORMAT:
{
  "_id": ObjectId("..."),
  "event_time": "2019-10-01 00:00:00",
  "event_type": "purchase",
  "product_id": 5300797,
  "category_id": 2053013553341792533,
  "category_code": "cosmetics.body.body_care",
  "brand": "runail",
  "price": 3.62,
  "user_id": 554748717,
  "user_session": "9333dfbd-b87a-4708-..."
}
```

**Expected Output:**

- "Connected successfully!" message
- 5000 documents inserted confirmation
- Sample JSON document displayed with all fields
- Query results showing filtered data
- Aggregation results showing brand statistics

### ✅ Gate Check P2-T1:

- [x]  PyMongo installed without errors
- [x]  Connection to Atlas successful
- [x]  5000 documents inserted (verified by `count_documents()`)
- [x]  Sample document displays all 9 fields as JSON
- [x]  At least one query and one aggregation run successfully

---

### P2-T2: Take Screenshots of MongoDB Atlas

**Who:** You (Lead) or Member 2

**Steps:**

1. Log into MongoDB Atlas web UI
2. Take screenshots of:
    - **Atlas Dashboard** showing the cluster name and status
    - **Browse Collections** view showing the `cosmetics_ecommerce` database and `events` collection
    - **A sample document** expanded in the Atlas UI
    - **Collection statistics** (document count, storage size)
3. Save screenshots as PNG/JPG files — these go into the report

**Expected Screenshots (4 total):**

1. Cluster overview with "Active" status
2. Database browser showing `cosmetics_ecommerce.events`
3. One expanded document showing all JSON fields
4. Collection stats showing document count = 5000 and storage size

### ✅ Gate Check P2-T2:

- [x]  4 screenshots saved and clearly readable
- [x]  Screenshots show correct database name, collection name, document count, and storage size
- [x]  At least one screenshot shows the JSON document structure

---

### P2-T3: Document NoSQL Concepts (Report Content)

**Who:** Member 2 writes this in the report. You provide the talking points below.

**Key points to cover in ~300 words:**

```
NOSQL CONCEPTS TO EXPLAIN:

1. SCHEMA FLEXIBILITY:
   - Unlike relational databases with fixed tables/columns, MongoDB stores
     documents as JSON (BSON internally)
   - Each document can have different fields — perfect for e-commerce where
     some products have category_code and others don't (null in our dataset)
   - No ALTER TABLE needed when data structure evolves

2. WHY NOSQL FOR BIG DATA:
   - Horizontal scaling (sharding) — distribute data across multiple servers
   - No complex JOIN operations — faster for read-heavy analytics
   - Native JSON format matches modern web APIs and event data
   - Handles unstructured/semi-structured data naturally

3. SCALABILITY & DISTRIBUTED STORAGE:
   - MongoDB Atlas scales automatically with data growth
   - Replica sets provide high availability
   - Sharding distributes data across clusters for performance
   - Perfect for high-volume event streams (millions of events/day)

4. COMPARISON WITH RELATIONAL (bonus marks differentiator):
   - RDBMS: Fixed schema, ACID transactions, complex queries via SQL
   - MongoDB: Flexible schema, eventual consistency, simple queries,
     better horizontal scaling
   - For our use case: event data is write-heavy, semi-structured,
     and doesn't need complex JOINs → NoSQL is the better fit
```

### ✅ Gate Check P2-T3:

- [x]  Schema flexibility explained with reference to the actual dataset (null category_code as example)
- [x]  Why NoSQL suits big data is explained (not just generic — tied to the project)
- [x]  Scalability benefits mentioned
- [x]  At least one comparison point with relational databases

---

## PHASE 2 — COMPLETE VERIFICATION

- [x]  MongoDB Atlas has 5000 documents in `cosmetics_ecommerce.events`
- [x]  Connection, insertion, query, and aggregation all demonstrated in notebook
- [x]  4 screenshots saved for the report (including collection stats with document count and storage size)
- [x]  NoSQL concepts documented with project-specific examples

---

## PHASE 3: Data Processing with Apache Spark (Step 3 — 20 Marks)

**Goal:** Perform comprehensive data cleaning, transformation, aggregation, and pattern analysis using Spark DataFrames. This is the highest-weighted technical step.

**Time Estimate:** 6–8 hours (largest phase — take breaks)

**Notebook:** `src/Main_Analysis.py` (Section 2 — continue below Phase 1 cells)

**⚠️ THIS PHASE CARRIES 20 MARKS. Invest the most time here. Every analytical task should have a clear business interpretation.**

**⚠️ NOTEBOOK CONTINUITY: Since this is in the same file as Phase 1, your `df` DataFrame is still available. No need to reload. Just add new `# COMMAND ----------` cells below your Phase 1 code.**

---

### P3-T1: Data Cleaning

**Who:** You (Lead)

### 🤖 Agent Prompt for P3-T1:

```
TASK: Clean and prepare the e-commerce dataset for analysis.

CONTEXT: Dataset loaded as Spark DataFrame with ~15-20M rows.
Known issues from the current project run: category_code is highly sparse (~98.29% null) and brand has substantial nulls (~42.32%).
event_time needs proper timestamp parsing.

WORKFLOW:
1. Verify the DataFrame is still available (it should be from Phase 1 in the same notebook):
   # If cluster restarted and df is lost, re-run Phase 1 cells above first
   # Or use this quick reload:
   from functools import reduce
   from pyspark.sql import functions as F
   from pyspark.sql.types import *

   try:
       row_count = df.count()
       print(f"DataFrame available: {row_count} rows")
   except:
       print("DataFrame lost — reloading...")
       DATA_PATH = "/Volumes/workspace/default/cosmetics_data"
       months = ["2019-Oct", "2019-Nov", "2019-Dec", "2020-Jan", "2020-Feb"]
       dfs = []
       for m in months:
           try:
               temp = spark.read.csv(f"{DATA_PATH}/{m}.csv", header=True, inferSchema=True)
               dfs.append(temp)
               print(f"Loaded {m}: {temp.count()} rows")
           except:
               print(f"Could not load {m}")
       df = reduce(lambda a, b: a.union(b), dfs)
       # Fix timestamp if needed
       if str(df.schema["event_time"].dataType) == "StringType":
           df = df.withColumn("event_time",
               F.to_timestamp(F.regexp_replace(F.col("event_time"), " UTC", ""),
                              "yyyy-MM-dd HH:mm:ss"))
       print(f"Total rows: {df.count()}")

2. Handle missing values:
   # Check nulls per column
   null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
   null_counts.show()

   # Strategy:
   # - category_code nulls → fill with "unknown"
   # - brand nulls → fill with "unknown"
   # - price nulls or 0 → drop those rows (price is essential)
   # - Drop rows where event_type is null

   df_clean = df.fillna({"category_code": "unknown", "brand": "unknown"})
   df_clean = df_clean.filter(F.col("price") > 0)
   df_clean = df_clean.filter(F.col("event_type").isNotNull())

   # NOTE: dropDuplicates() on 20M rows is feasible on Serverless (unlike CE).
   # However, event data rarely has true duplicates (timestamped to the second),
   # so it's optional. If you want to be thorough:
   # df_clean = df_clean.dropDuplicates(["event_time", "user_id", "product_id", "event_type"])
   # This may take 5-10 min but won't OOM on Serverless.

   print(f"Rows after cleaning: {df_clean.count()}")
   print(f"Rows removed: {df.count() - df_clean.count()}")

3. Feature engineering — extract time features:
   df_clean = df_clean.withColumn("event_date", F.to_date("event_time"))
   df_clean = df_clean.withColumn("event_hour", F.hour("event_time"))
   df_clean = df_clean.withColumn("event_dayofweek", F.dayofweek("event_time"))
   df_clean = df_clean.withColumn("event_month", F.month("event_time"))

   # Extract main category from category_code (first part before '.')
   df_clean = df_clean.withColumn(
       "main_category",
       F.when(F.col("category_code") != "unknown",
              F.split(F.col("category_code"), "\\\\.").getItem(0))
        .otherwise("unknown")
   )

   df_clean.printSchema()
   df_clean.show(5)

4. Cache the cleaned DataFrame for faster downstream operations:
   df_clean.cache()
   print(f"Cleaned dataset cached. Total rows: {df_clean.count()}")

ARTIFACTS:
- Null analysis before and after cleaning
- Row count before and after (shows data quality improvement)
- New derived columns visible in schema
- Cached DataFrame ready for analysis

EXPECTED OUTCOME:
- Row removal depends on the cleaning strategy (fill-vs-drop); do not force a fixed percentage target
- 4 new columns: event_date, event_hour, event_dayofweek, main_category
- No nulls remaining in critical columns
```

**Expected Output:**

- Print showing rows before and after cleaning (e.g., "20M → 18M, removed 2M rows")
- Schema showing new derived columns
- Sample rows showing clean data with new columns

### ✅ Gate Check P3-T1:

- [ ]  Cleaning steps are clearly documented with reasons
- [ ]  Null handling strategy explained (why fill vs drop)
- [ ]  Row count before/after cleaning printed
- [ ]  Derived columns (event_date, event_hour, event_dayofweek, main_category) created
- [ ]  DataFrame cached successfully

---

### P3-T2: Descriptive Analytics — Aggregations & Patterns

**Who:** You (Lead)

### 🤖 Agent Prompt for P3-T2:

```
TASK: Perform descriptive analytics on the cleaned e-commerce dataset.
This is the core of Step 3 (20 marks). Every analysis must have a clear
business interpretation written as a markdown cell after the code.

CONTEXT: df_clean is the cached, cleaned Spark DataFrame.

WORKFLOW — Perform ALL of the following analyses:

ANALYSIS 1: Event Type Distribution
   event_dist = df_clean.groupBy("event_type").count().orderBy("count", ascending=False)
   event_dist.show()
   # Calculate percentages
   total = df_clean.count()
   event_dist_pct = event_dist.withColumn("percentage", F.round(F.col("count") / total * 100, 2))
   event_dist_pct.show()

   INTERPRETATION: "Views dominate at ~X%, with only ~Y% converting to purchases.
   This indicates a conversion rate of Y%, which is [above/below] industry average of ~2-3%."

ANALYSIS 2: Revenue Analysis
   # Total revenue
   revenue = df_clean.filter(F.col("event_type") == "purchase").agg(F.sum("price").alias("total_revenue"))
   revenue.show()

   # Revenue by month — ⚠️ MUST use event_date for correct ordering, NOT event_month
   # event_month alone puts Jan(1), Feb(2) before Oct(10) — wrong order!
   monthly_revenue = df_clean.filter(F.col("event_type") == "purchase") \\
       .withColumn("year_month", F.date_format("event_time", "yyyy-MM")) \\
       .groupBy("year_month") \\
       .agg(
           F.sum("price").alias("total_revenue"),
           F.count("*").alias("num_purchases"),
           F.avg("price").alias("avg_purchase_price")
       ).orderBy("year_month")
   monthly_revenue.show()

   INTERPRETATION: "Monthly revenue shows [trend]. November may show a spike
   due to Black Friday/holiday shopping. January shows [decline/increase]
   possibly due to post-holiday reduced spending."

ANALYSIS 3: Top 10 Most Popular Products (by views and by purchases)
   # By views
   top_viewed = df_clean.filter(F.col("event_type") == "view") \\
       .groupBy("product_id", "brand", "category_code") \\
       .count().orderBy("count", ascending=False).limit(10)
   top_viewed.show()

   # By purchases
   top_purchased = df_clean.filter(F.col("event_type") == "purchase") \\
       .groupBy("product_id", "brand", "category_code") \\
       .count().orderBy("count", ascending=False).limit(10)
   top_purchased.show()

   INTERPRETATION: "The most viewed products are [X], while most purchased are [Y].
   Discrepancies between views and purchases suggest [insight about conversion]."

ANALYSIS 4: Top 10 Brands by Revenue
   brand_revenue = df_clean.filter(F.col("event_type") == "purchase") \\
       .groupBy("brand") \\
       .agg(
           F.sum("price").alias("total_revenue"),
           F.count("*").alias("num_purchases"),
           F.avg("price").alias("avg_price")
       ).orderBy("total_revenue", ascending=False).limit(10)
   brand_revenue.show()

   INTERPRETATION: "Brand [X] generates the most revenue at $X.
   The top 10 brands account for X% of total revenue."

ANALYSIS 5: Hourly Activity Patterns (Busiest Hours)
   hourly = df_clean.groupBy("event_hour") \\
       .agg(
           F.count("*").alias("total_events"),
           F.count(F.when(F.col("event_type") == "purchase", True)).alias("purchases")
       ).orderBy("event_hour")
   hourly.show(24)

   INTERPRETATION: "Peak browsing occurs at [X] hours, while peak purchasing is at [Y].
   This suggests optimal times for marketing campaigns and flash sales."

ANALYSIS 6: Day of Week Patterns
   daily = df_clean.groupBy("event_dayofweek") \\
       .agg(
           F.count("*").alias("total_events"),
           F.count(F.when(F.col("event_type") == "purchase", True)).alias("purchases")
       ).orderBy("event_dayofweek")
   daily.show()

   INTERPRETATION: "[Weekday/Weekend] shows higher purchase rates, suggesting..."

ANALYSIS 7: Conversion Funnel (View → Cart → Purchase)
   # This groupBy("user_session") should complete in 5-10 min on Serverless.
   # If it errors, use: df_sample = df_clean.sample(0.3, seed=42) instead.
   # Per user_session: track how many sessions have views, carts, purchases
   session_funnel = df_clean.groupBy("user_session") \\
       .agg(
           F.max(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("has_view"),
           F.max(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("has_cart"),
           F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("has_purchase")
       )

   total_sessions = session_funnel.count()
   viewed = session_funnel.filter(F.col("has_view") == 1).count()
   carted = session_funnel.filter(F.col("has_cart") == 1).count()
   purchased = session_funnel.filter(F.col("has_purchase") == 1).count()

   print(f"Total Sessions: {total_sessions}")
   print(f"Sessions with Views: {viewed} ({viewed/total_sessions*100:.2f}%)")
   print(f"Sessions with Cart: {carted} ({carted/total_sessions*100:.2f}%)")
   print(f"Sessions with Purchase: {purchased} ({purchased/total_sessions*100:.2f}%)")
   print(f"View→Cart Rate: {carted/viewed*100:.2f}%")
   print(f"Cart→Purchase Rate: {purchased/carted*100:.2f}%")

   INTERPRETATION: "The conversion funnel shows X% of sessions add to cart,
   and Y% of those complete a purchase. Cart abandonment rate is Z%."

ANALYSIS 8: Average Basket Value
   basket = df_clean.filter(F.col("event_type") == "purchase") \\
       .groupBy("user_session") \\
       .agg(
           F.sum("price").alias("basket_total"),
           F.count("*").alias("items_count")
       )
   basket.describe("basket_total", "items_count").show()

   INTERPRETATION: "Average basket value is $X with Y items per order."

ARTIFACTS:
- 8 distinct analyses with code + output + interpretation
- Each analysis has a markdown cell explaining the business meaning
- All outputs visible and formatted

EXPECTED OUTCOME:
- Conversion rate around 2-5%
- Clear hourly and daily patterns
- Top brands and products identified
- Revenue trends across months
```

**Expected Output for each analysis:**

- Code cell with output table/numbers
- Markdown cell below with 2-3 sentence business interpretation
- 8 total analysis blocks in the notebook

### ✅ Gate Check P3-T2:

- [ ]  All 8 analyses execute without errors
- [ ]  Each analysis has both code output AND written interpretation
- [ ]  Numbers make business sense (e.g., views > carts > purchases)
- [ ]  Conversion funnel shows realistic rates (1-5% is typical for e-commerce)
- [ ]  Revenue figures are non-zero and reasonable
- [ ]  Top brands/products are named and discussed

---

## PHASE 3 — COMPLETE VERIFICATION

- [ ]  Data cleaning documented with before/after row counts
- [ ]  8+ distinct analytical tasks completed
- [ ]  Every analysis has a business interpretation
- [ ]  Notebook runs top-to-bottom without errors
- [ ]  Results are cached for use in subsequent phases

---

## PHASE 4: Data Visualization (Step 4 — 10 Marks)

**Goal:** Create at least 3 meaningful visualizations. The brief says "at least three" — we'll do 5 for extra credit without over-engineering.

**Time Estimate:** 2–3 hours

**Notebook:** `src/Main_Analysis.py` (Section 3 — continue below Phase 3 cells)

**⚠️ NOTEBOOK CONTINUITY: `df_clean` is still available from Phase 3 above in the same file.**

---

### P4-T1: Create Visualizations

**Who:** You (Lead)

### 🤖 Agent Prompt for P4-T1:

```
TASK: Create 5 data visualizations from the e-commerce dataset using
matplotlib/seaborn in Databricks. Each visualization must be clearly
titled and interpreted.

CONTEXT: df_clean is the cached, cleaned DataFrame from Phase 3.
We need to convert to Pandas for matplotlib/seaborn (use samples or
aggregated data — never convert the full 20M rows to Pandas).

SETUP:
   # matplotlib and seaborn are typically pre-installed on Databricks Runtime
   # but install just in case:
   # %pip install seaborn matplotlib
   import matplotlib.pyplot as plt
   import seaborn as sns
   import pandas as pd

WORKFLOW:

VISUALIZATION 1: BAR CHART — Top 10 Brands by Revenue
   brand_rev = df_clean.filter(F.col("event_type") == "purchase") \\
       .groupBy("brand") \\
       .agg(F.sum("price").alias("revenue")) \\
       .orderBy("revenue", ascending=False).limit(10)

   brand_rev_pd = brand_rev.toPandas()

   plt.figure(figsize=(12, 6))
   sns.barplot(data=brand_rev_pd, x="revenue", y="brand", palette="viridis")
   plt.title("Top 10 Brands by Revenue", fontsize=16)
   plt.xlabel("Total Revenue ($)")
   plt.ylabel("Brand")
   plt.tight_layout()
   plt.show()

   INTERPRETATION (markdown cell below):
   "This bar chart reveals that [Brand X] dominates revenue generation
   in the cosmetics store. The top 3 brands account for approximately X%
   of total revenue, suggesting a highly concentrated market."

VISUALIZATION 2: LINE CHART — Monthly Revenue Trend
   # ⚠️ Use year_month string for correct chronological order
   monthly_rev = df_clean.filter(F.col("event_type") == "purchase") \\
       .withColumn("year_month", F.date_format("event_time", "yyyy-MM")) \\
       .groupBy("year_month") \\
       .agg(F.sum("price").alias("revenue")) \\
       .orderBy("year_month")

   monthly_rev_pd = monthly_rev.toPandas()
   # Create readable labels
   label_map = {"2019-10": "Oct '19", "2019-11": "Nov '19", "2019-12": "Dec '19",
                "2020-01": "Jan '20", "2020-02": "Feb '20"}
   monthly_rev_pd["month_label"] = monthly_rev_pd["year_month"].map(label_map)

   plt.figure(figsize=(10, 5))
   plt.plot(monthly_rev_pd["month_label"], monthly_rev_pd["revenue"],
            marker='o', linewidth=2, markersize=8, color='#2E86AB')
   plt.title("Monthly Revenue Trend (Oct 2019 – Feb 2020)", fontsize=16)
   plt.xlabel("Month")
   plt.ylabel("Revenue ($)")
   plt.grid(True, alpha=0.3)
   plt.tight_layout()
   plt.show()

   INTERPRETATION: "Revenue shows a [pattern]. November may show a spike
   due to Black Friday/holiday shopping. January shows [decline/increase]
   possibly due to post-holiday reduced spending."

VISUALIZATION 3: PIE CHART — Event Type Distribution
   event_dist = df_clean.groupBy("event_type").count()
   event_pd = event_dist.toPandas()

   plt.figure(figsize=(8, 8))
   colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D']
   plt.pie(event_pd["count"], labels=event_pd["event_type"], autopct='%1.1f%%',
           colors=colors, startangle=140, textprops={'fontsize': 12})
   plt.title("Distribution of Event Types", fontsize=16)
   plt.tight_layout()
   plt.show()

   INTERPRETATION: "Views constitute the vast majority (~X%) of all events,
   with purchases making up only ~Y%. This highlights the conversion
   challenge in e-commerce — most visitors browse without buying."

VISUALIZATION 4: BAR CHART — Hourly Purchase Pattern
   hourly_purchases = df_clean.filter(F.col("event_type") == "purchase") \\
       .groupBy("event_hour").count().orderBy("event_hour")
   hourly_pd = hourly_purchases.toPandas()

   plt.figure(figsize=(14, 6))
   sns.barplot(data=hourly_pd, x="event_hour", y="count", palette="RdYlGn_r")
   plt.title("Purchase Activity by Hour of Day (UTC)", fontsize=16)
   plt.xlabel("Hour of Day")
   plt.ylabel("Number of Purchases")
   plt.tight_layout()
   plt.show()

   INTERPRETATION: "Peak purchasing hours are between X:00 and Y:00 UTC.
   This insight can drive targeted marketing campaigns and flash sales
   timing to maximize conversion."

VISUALIZATION 5: BAR CHART — Top 10 Product Categories by Purchase Count
   cat_purchases = df_clean.filter(
       (F.col("event_type") == "purchase") & (F.col("main_category") != "unknown")
   ).groupBy("main_category").count().orderBy("count", ascending=False).limit(10)
   cat_pd = cat_purchases.toPandas()

   plt.figure(figsize=(12, 6))
   sns.barplot(data=cat_pd, x="count", y="main_category", palette="coolwarm")
   plt.title("Top 10 Product Categories by Purchase Volume", fontsize=16)
   plt.xlabel("Number of Purchases")
   plt.ylabel("Category")
   plt.tight_layout()
   plt.show()

   INTERPRETATION: "The cosmetics category leads purchases, followed by [X].
   This reflects the store's core business focus and can guide inventory management."

IMPORTANT:
- Use display() in Databricks for inline chart display, or plt.show()
- Alternatively, use Databricks built-in visualization:
  After running a query, click the chart icon below the output table
  to create bar/line/pie charts directly. Both approaches are valid.

ARTIFACTS:
- 5 visualizations with clear titles, labels, and readable colors
- 5 interpretation markdown cells (one after each visualization)
```

**Expected Output:**

- 5 distinct chart images rendered in the notebook
- Each chart has a title, axis labels, and readable formatting
- Each chart has a 2-3 sentence interpretation below it

### ✅ Gate Check P4-T1:

- [x]  5 visualizations rendered without errors (minimum 3 required, but 5 is better)
- [x]  At least 3 different chart types used (bar, line, pie)
- [x]  Every chart has a title and axis labels
- [x]  Every chart has a written interpretation
- [x]  Charts tell a coherent story about the business
- [x]  No blank/empty charts — all show meaningful data

---

## PHASE 4 — COMPLETE VERIFICATION

- [x]  5 visualizations created and visible in the notebook
- [x]  At least 3 chart types (bar, line, pie)
- [x]  Each visualization has a written interpretation
- [x]  Charts use real data from the dataset (not dummy data)

---

## PHASE 5: Predictive Analytics with Spark ML (Step 5 — 15 Marks)

**Goal:** Build, compare, and evaluate multiple machine learning models. This is the differentiator for 80%+ marks — comparing models and showing selection rationale.

**Time Estimate:** 5–7 hours

**Notebook:** `src/Main_Analysis.py` (Section 4 — continue below Phase 4 cells)

**⚠️ KEY DIFFERENTIATOR: The brief says "the best machine learning algorithm" and "show how the model was selected." Your module leader specifically said she'd be happy to see model comparison. This is where you separate from average submissions.**

**⚠️ NOTEBOOK CONTINUITY: `df_clean` is still available from Phase 3 above in the same file.**

---

### P5-T1: Feature Engineering for ML

**Who:** You (Lead)

### 🤖 Agent Prompt for P5-T1:

```
TASK: Prepare features for a binary classification problem — predicting
whether a user session will result in a purchase.

CONTEXT: We need to transform raw event data into session-level features
that can be fed into Spark MLlib classifiers.

PROBLEM FORMULATION:
- Unit of prediction: user_session
- Label: 1 if session contains a purchase event, 0 otherwise
- Features: aggregated session-level behavior metrics

WORKFLOW:
1. Create session-level features:
   from pyspark.sql import functions as F
   from pyspark.sql.window import Window

   # ⚠️ PERFORMANCE NOTE: groupBy("user_session") with countDistinct on
   # 15-20M rows. On Serverless compute this should complete in 5-15 min
   # (much faster than Community Edition's single node).
   #
   # FALLBACK: If it still takes >20 min or errors out, sample first:
   #   df_ml = df_clean.sample(0.3, seed=42)  # Use 30% of data
   #   Then use df_ml instead of df_clean below.
   #   Document this in the report: "Due to compute constraints,
   #   a 30% stratified sample was used for ML training."

   session_features = df_clean.groupBy("user_session").agg(
       # Label: did this session have a purchase?
       F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("label"),

       # Feature: total number of events in session
       F.count("*").alias("total_events"),

       # Feature: number of views
       F.count(F.when(F.col("event_type") == "view", True)).alias("view_count"),

       # Feature: number of cart additions
       F.count(F.when(F.col("event_type") == "cart", True)).alias("cart_count"),

       # Feature: number of cart removals
       F.count(F.when(F.col("event_type") == "remove_from_cart", True)).alias("remove_cart_count"),

       # Feature: number of unique products viewed
       F.countDistinct(F.when(F.col("event_type") == "view", F.col("product_id"))).alias("unique_products_viewed"),

       # Feature: number of unique brands interacted with
       F.countDistinct("brand").alias("unique_brands"),

       # Feature: average price of products interacted with
       F.avg("price").alias("avg_price"),

       # Feature: max price of products interacted with
       F.max("price").alias("max_price"),

       # Feature: min price
       F.min("price").alias("min_price"),

       # Feature: hour of first event (session start time)
       F.min("event_hour").alias("session_start_hour"),

       # Feature: day of week
       F.first("event_dayofweek").alias("day_of_week"),
   )

   print(f"Total sessions: {session_features.count()}")
   print(f"Purchase sessions (label=1): {session_features.filter(F.col('label')==1).count()}")
   print(f"Non-purchase sessions (label=0): {session_features.filter(F.col('label')==0).count()}")

   session_features.show(10)

2. Handle class imbalance info:
   # Note the imbalance ratio — important for model evaluation
   purchase_count = session_features.filter(F.col("label") == 1).count()
   total_count = session_features.count()
   print(f"Class balance: {purchase_count/total_count*100:.2f}% positive (purchase)")
   # Expected: ~2-5% positive — highly imbalanced

3. Assemble features vector using Spark MLlib:
   from pyspark.ml.feature import VectorAssembler

   feature_columns = [
       "total_events", "view_count", "cart_count", "remove_cart_count",
       "unique_products_viewed", "unique_brands", "avg_price", "max_price",
       "min_price", "session_start_hour", "day_of_week"
   ]

   assembler = VectorAssembler(inputCols=feature_columns, outputCol="features",
                               handleInvalid="skip")  # Skip rows with NaN/null — prevents crashes
   ml_data = assembler.transform(session_features).select("features", "label")
   ml_data.show(5)

4. Split into train/test:
   train_data, test_data = ml_data.randomSplit([0.8, 0.2], seed=42)
   print(f"Training set: {train_data.count()} rows")
   print(f"Test set: {test_data.count()} rows")

   # Cache for faster training
   train_data.cache()
   test_data.cache()

ARTIFACTS:
- session_features DataFrame with 11 features + 1 label
- Class balance analysis printed
- Assembled features vector
- Train/test split ready

EXPECTED OUTCOME:
- Hundreds of thousands of sessions
- ~2-5% positive class (purchase sessions)
- 11 features per session
- 80/20 train/test split
```

**Expected Output:**

```
Total sessions: ~500,000-2,000,000 (depends on data)
Purchase sessions (label=1): ~20,000-100,000
Non-purchase sessions (label=0): ~400,000-1,900,000
Class balance: ~3% positive (purchase)

Feature vector assembled with 11 numeric features
Training set: ~X rows
Test set: ~Y rows
```

### ✅ Gate Check P5-T1:

- [ ]  Session-level features created (not row-level)
- [ ]  Label column is binary (0/1)
- [ ]  11 features + 1 label column
- [ ]  Class balance printed and noted (expect heavy imbalance)
- [ ]  VectorAssembler runs without errors
- [ ]  Train/test split created with seed for reproducibility

---

### P5-T2: Train and Compare Multiple Models

**Who:** You (Lead)

### 🤖 Agent Prompt for P5-T2:

```
TASK: Train 3 classification models, compare their performance, and select
the best one. This is the key differentiator for 80%+ marks.

MODELS TO TRAIN:
1. Logistic Regression
2. Decision Tree Classifier
3. Random Forest Classifier

WORKFLOW:
1. Train Logistic Regression:
   from pyspark.ml.classification import LogisticRegression
   from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

   lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100)
   lr_model = lr.fit(train_data)
   lr_predictions = lr_model.transform(test_data)

   print("=== Logistic Regression Results ===")
   lr_predictions.select("label", "prediction", "probability").show(10)

2. Train Decision Tree:
   from pyspark.ml.classification import DecisionTreeClassifier

   dt = DecisionTreeClassifier(featuresCol="features", labelCol="label", maxDepth=10)
   dt_model = dt.fit(train_data)
   dt_predictions = dt_model.transform(test_data)

   print("=== Decision Tree Results ===")
   dt_predictions.select("label", "prediction", "probability").show(10)

3. Train Random Forest:
   from pyspark.ml.classification import RandomForestClassifier

   # ⚠️ numTrees=50 balances performance vs training time on Serverless.
   # Serverless has significantly more resources than Community Edition,
   # so we can use more trees than CE's limit of 20.
   # If training takes >15 min, reduce to numTrees=20.
   rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50, maxDepth=10)
   rf_model = rf.fit(train_data)
   rf_predictions = rf_model.transform(test_data)

   print("=== Random Forest Results ===")
   rf_predictions.select("label", "prediction", "probability").show(10)

4. Evaluate ALL three models with same metrics:

   def evaluate_model(predictions, model_name):
       """Evaluate a model and return metrics dictionary"""
       # Binary evaluator
       binary_eval = BinaryClassificationEvaluator(labelCol="label")
       auc = binary_eval.evaluate(predictions, {binary_eval.metricName: "areaUnderROC"})

       # Multiclass evaluator for accuracy, precision, recall, f1
       mc_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
       accuracy = mc_eval.evaluate(predictions, {mc_eval.metricName: "accuracy"})
       precision = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedPrecision"})
       recall = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedRecall"})
       f1 = mc_eval.evaluate(predictions, {mc_eval.metricName: "f1"})

       print(f"\\n{'='*50}")
       print(f"Model: {model_name}")
       print(f"{'='*50}")
       print(f"Accuracy:  {accuracy:.4f}")
       print(f"Precision: {precision:.4f}")
       print(f"Recall:    {recall:.4f}")
       print(f"F1 Score:  {f1:.4f}")
       print(f"AUC-ROC:   {auc:.4f}")

       return {
           "Model": model_name,
           "Accuracy": round(accuracy, 4),
           "Precision": round(precision, 4),
           "Recall": round(recall, 4),
           "F1 Score": round(f1, 4),
           "AUC-ROC": round(auc, 4)
       }

   lr_metrics = evaluate_model(lr_predictions, "Logistic Regression")
   dt_metrics = evaluate_model(dt_predictions, "Decision Tree")
   rf_metrics = evaluate_model(rf_predictions, "Random Forest")

5. Create comparison table:
   comparison_df = spark.createDataFrame([lr_metrics, dt_metrics, rf_metrics])
   comparison_df.show()

   # Also create a Pandas version for nice display
   comparison_pd = pd.DataFrame([lr_metrics, dt_metrics, rf_metrics])
   print("\\n=== MODEL COMPARISON TABLE ===")
   print(comparison_pd.to_string(index=False))

6. Feature importance from the best model (likely Random Forest):
   # Random Forest feature importance
   importances = rf_model.featureImportances.toArray()
   feature_importance_list = list(zip(feature_columns, importances))
   feature_importance_list.sort(key=lambda x: x[1], reverse=True)

   print("\\n=== Feature Importance (Random Forest) ===")
   for feat, imp in feature_importance_list:
       print(f"{feat:30s}: {imp:.4f}")

7. Model selection justification (markdown cell):
   """
   MODEL SELECTION RATIONALE:

   We trained three classification models and compared their performance:

   [Insert comparison table]

   Random Forest achieved the highest [F1/AUC] score of X.XX, outperforming
   Logistic Regression (X.XX) and Decision Tree (X.XX).

   Random Forest is the best choice because:
   1. It handles class imbalance better through ensemble voting
   2. It captures non-linear relationships between features
   3. It provides feature importance scores for business interpretability
   4. It is less prone to overfitting than a single Decision Tree

   The top 3 most important features for predicting purchases are:
   1. cart_count — [interpretation]
   2. total_events — [interpretation]
   3. unique_products_viewed — [interpretation]
   """

ARTIFACTS:
- 3 trained models with predictions
- Evaluation metrics for all 3 models
- Comparison table (critical for marks)
- Feature importance ranking
- Model selection justification paragraph

EXPECTED OUTCOME:
- Random Forest likely performs best (F1/AUC highest)
- Accuracy will be high (~95%+) but misleading due to class imbalance
- Focus the interpretation on F1, Precision, Recall, and AUC — not just accuracy
- Feature importance will likely show cart_count as #1 predictor
```

**Expected Output:**

```
=== MODEL COMPARISON TABLE ===
            Model  Accuracy  Precision  Recall  F1 Score  AUC-ROC
Logistic Regression   0.95XX    0.93XX   0.95XX    0.94XX   0.82XX
     Decision Tree   0.94XX    0.92XX   0.94XX    0.93XX   0.78XX
     Random Forest   0.96XX    0.95XX   0.96XX    0.95XX   0.88XX

=== Feature Importance (Random Forest) ===
cart_count                    : 0.35XX
total_events                  : 0.15XX
unique_products_viewed        : 0.12XX
avg_price                     : 0.10XX
...
```

### ✅ Gate Check P5-T2:

- [ ]  All 3 models train without errors
- [ ]  Each model produces predictions on test data
- [ ]  5 metrics calculated for each model (Accuracy, Precision, Recall, F1, AUC)
- [ ]  Comparison table created and formatted
- [ ]  Feature importance extracted from Random Forest
- [ ]  Model selection rationale written with clear reasoning
- [ ]  Interpretation acknowledges class imbalance and why accuracy alone is misleading

---

### P5-T3: (BONUS) K-Means Customer Segmentation

**Who:** You (Lead)

**⚠️ OPTIONAL BUT RECOMMENDED:** This adds clustering alongside classification, demonstrating breadth of ML knowledge. Only attempt if Phase 5 T1-T2 are fully complete.

**Time Estimate:** 1–2 hours

### 🤖 Agent Prompt for P5-T3:

```
TASK: Apply K-Means clustering to segment customers based on behavior.
This is a bonus analysis showing both classification and clustering competency.

WORKFLOW:
1. Create user-level features (different from session-level):
   user_features = df_clean.groupBy("user_id").agg(
       F.count("*").alias("total_events"),
       F.count(F.when(F.col("event_type") == "purchase", True)).alias("purchase_count"),
       F.count(F.when(F.col("event_type") == "view", True)).alias("view_count"),
       F.countDistinct("user_session").alias("num_sessions"),
       F.avg("price").alias("avg_price"),
       F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0)).alias("total_spend"),
       F.countDistinct("brand").alias("unique_brands")
   )

   # Filter out users with no activity (just in case)
   user_features = user_features.filter(F.col("total_events") > 0)

2. Assemble and scale features:
   from pyspark.ml.feature import VectorAssembler, StandardScaler

   user_feature_cols = ["total_events", "purchase_count", "view_count",
                        "num_sessions", "avg_price", "total_spend", "unique_brands"]

   assembler = VectorAssembler(inputCols=user_feature_cols, outputCol="raw_features",
                               handleInvalid="skip")
   user_data = assembler.transform(user_features)

   # ⚠️ withMean MUST be False — Spark's StandardScaler with withMean=True
   # requires DenseVector but VectorAssembler produces SparseVector → crash
   scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                           withStd=True, withMean=False)
   scaler_model = scaler.fit(user_data)
   user_data_scaled = scaler_model.transform(user_data)

3. Train K-Means with k=3 (Low, Medium, High value customers):
   from pyspark.ml.clustering import KMeans
   from pyspark.ml.evaluation import ClusteringEvaluator

   kmeans = KMeans(featuresCol="features", k=3, seed=42)
   km_model = kmeans.fit(user_data_scaled)
   km_predictions = km_model.transform(user_data_scaled)

   # Evaluate with Silhouette Score
   evaluator = ClusteringEvaluator(featuresCol="features")
   silhouette = evaluator.evaluate(km_predictions)
   print(f"Silhouette Score: {silhouette:.4f}")

4. Analyze clusters:
   cluster_summary = km_predictions.groupBy("prediction").agg(
       F.count("*").alias("num_customers"),
       F.avg("total_events").alias("avg_events"),
       F.avg("purchase_count").alias("avg_purchases"),
       F.avg("total_spend").alias("avg_spend"),
       F.avg("num_sessions").alias("avg_sessions")
   ).orderBy("prediction")
   cluster_summary.show()

   # Label clusters based on characteristics:
   # Cluster with highest avg_spend → "High-Value Customers"
   # Cluster with medium → "Regular Customers"
   # Cluster with lowest → "Casual Browsers"

ARTIFACTS:
- K-Means model with k=3
- Silhouette score
- Cluster summary table with business labels
- Interpretation of each customer segment

EXPECTED OUTCOME:
- Silhouette score: 0.3-0.7 (acceptable range)
- 3 distinct customer segments with clear behavioral differences
- Actionable insights: "High-value segment represents X% of users but Y% of revenue"
```

### ✅ Gate Check P5-T3:

- [ ]  K-Means trains without errors
- [ ]  Silhouette score printed and interpreted
- [ ]  Cluster summary shows distinct differences between groups
- [ ]  Business labels assigned to each cluster
- [ ]  This section clearly labeled as "Additional Analysis" in the notebook

---

## PHASE 5 — COMPLETE VERIFICATION

- [x]  Session-level features created with 11 features
- [x]  3 classification models trained (LR, DT, RF)
- [x]  Comparison table with 5 metrics per model
- [x]  Best model selected with written justification
- [x]  Feature importance extracted and interpreted
- [x]  (Bonus) K-Means clustering with customer segments
- [x]  All code runs without errors in the notebook

---

## PHASE 6: Big Data Architecture Diagram (Step 6 — 10 Marks)

**Goal:** Design a professional architecture diagram showing how this system would work in a real organization.

**Time Estimate:** 1–2 hours

---

### P6-T1: Create Architecture Diagram

**Who:** You (Lead) or Member 3

**Tool Options:**

- [Draw.io](http://draw.io/) ([https://draw.io](https://draw.io/)) — free, export as PNG
- Lucidchart — free tier available
- PowerPoint/Google Slides — simple shapes work fine
- Canva — has diagram templates

**Architecture to Diagram:**

```
┌─────────────────────────────────────────────────────────────────┐
│                    BIG DATA ARCHITECTURE                         │
│              Cosmetics E-Commerce Analytics Platform              │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────┐                                                │
│  │  DATA SOURCES │                                                │
│  │  • Website    │                                                │
│  │    Clickstream│                                                │
│  │  • Mobile App │                                                │
│  │  • CRM System │                                                │
│  └──────┬───────┘                                                │
│         │                                                         │
│         ▼                                                         │
│  ┌──────────────────┐     ┌──────────────────┐                   │
│  │  DATA INGESTION   │     │  NoSQL STORAGE    │                   │
│  │  • Apache Kafka   │────▶│  • MongoDB Atlas  │                   │
│  │  • Event Streaming│     │  • Raw JSON events│                   │
│  └──────┬───────────┘     └──────────────────┘                   │
│         │                                                         │
│         ▼                                                         │
│  ┌──────────────────┐                                             │
│  │  DATA PROCESSING  │                                             │
│  │  • Apache Spark   │                                             │
│  │  • Databricks     │                                             │
│  │  • Data Cleaning  │                                             │
│  │  • Aggregations   │                                             │
│  └──────┬───────────┘                                             │
│         │                                                         │
│         ▼                                                         │
│  ┌──────────────────┐                                             │
│  │  MACHINE LEARNING │                                             │
│  │  • Spark MLlib    │                                             │
│  │  • Random Forest  │                                             │
│  │  • Customer Seg.  │                                             │
│  └──────┬───────────┘                                             │
│         │                                                         │
│         ▼                                                         │
│  ┌──────────────────┐     ┌──────────────────┐                   │
│  │  ANALYTICS OUTPUT │     │  BUSINESS ACTIONS │                   │
│  │  • Dashboards     │────▶│  • Marketing      │                   │
│  │  • Visualizations │     │  • Inventory Mgmt │                   │
│  │  • Reports        │     │  • Personalization│                   │
│  └──────────────────┘     └──────────────────┘                   │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

**Component Explanations (each needs 1-2 sentences in the report):**

| Component | Role |
| --- | --- |
| Data Sources | Website clickstream, mobile app events, and CRM data generate raw user interaction events in real-time |
| Data Ingestion (Kafka) | Apache Kafka streams event data in real-time, handling high throughput from millions of concurrent users |
| NoSQL Storage (MongoDB) | MongoDB Atlas stores raw event data as JSON documents, providing schema flexibility and horizontal scalability |
| Data Processing (Spark/Databricks) | Apache Spark on Databricks performs distributed data cleaning, transformation, and aggregation at scale |
| Machine Learning (Spark MLlib) | MLlib trains classification models (purchase prediction) and clustering models (customer segmentation) on processed data |
| Analytics Output | Interactive dashboards and visualizations communicate insights to business stakeholders |
| Business Actions | Data-driven decisions in marketing, inventory management, and customer personalization |

### ✅ Gate Check P6-T1:

- [ ]  Architecture diagram created as a PNG/JPG image
- [ ]  All 6-7 components are clearly labeled
- [ ]  Arrows show data flow direction
- [ ]  Diagram is professional and readable (not hand-drawn)
- [ ]  Component explanations written (1-2 sentences each)
- [ ]  Diagram saved for inclusion in the report

---

## PHASE 6 — COMPLETE VERIFICATION

- [ ]  Architecture diagram created and exported as image
- [ ]  Each component has a written explanation
- [ ]  Diagram covers the full pipeline: Source → Storage → Processing → ML → Output → Action

---

## PHASE 7: Report Writing (Approx. 2500 Words)

**Goal:** Write the final report covering all 6 steps, with Harvard referencing.

**Time Estimate:** 4–6 hours (split across 4 members)

**Format:** Word document (.docx), Harvard referencing, ~2500 words

---

### P7-T1: Report Structure

**Who:** All 4 members (each writes their assigned section)

**Structure:**

```
REPORT STRUCTURE (~2500 words total)

1. INTRODUCTION (~200 words) — Member 4
   - Module context (Big Data Analytics, PUSL3121)
   - Brief overview of the project
   - Business problem statement
   - Report structure overview

2. DATASET SELECTION AND INDUSTRY CONTEXT (~350 words) — Member 2
   - Dataset description (source, size, columns, time range)
   - E-commerce industry context
   - Business problem: predicting purchase conversion
   - How analytics addresses this problem
   - Link to descriptive/predictive/prescriptive analytics

3. DATA STORAGE USING NOSQL (~300 words) — Member 2
   - MongoDB Atlas setup description
   - Sample document format (include JSON example)
   - Screenshots (reference them as Figure 1, 2, etc.)
   - Schema flexibility explanation
   - Why NoSQL suits this big data use case
   - Scalability benefits

4. DATA PROCESSING WITH APACHE SPARK (~500 words) — You (Lead)
   - Data loading process (5 CSV files, union approach)
   - Data cleaning steps and rationale
   - Key analytical findings (summarize the 8 analyses):
     • Event distribution and conversion rates
     • Revenue analysis and trends
     • Top brands and products
     • Temporal patterns (hourly, daily)
     • Conversion funnel metrics
   - Business interpretation of findings

5. DATA VISUALIZATION (~300 words) — Member 3
   - Description of each visualization
   - What each reveals about the data
   - Patterns and trends observed
   - Reference figures by number

6. PREDICTIVE ANALYTICS USING SPARK ML (~500 words) — You (Lead)
   - Feature engineering approach (session-level features)
   - Models trained (LR, DT, RF)
   - Evaluation metrics and comparison table
   - Model selection rationale
   - Feature importance analysis
   - (Bonus) Customer segmentation results
   - Business implications of predictions

7. BIG DATA ARCHITECTURE DESIGN (~200 words) — Member 3
   - Architecture diagram (reference as Figure)
   - Role of each component
   - How it would operate in a real organization

8. CONCLUSION AND BUSINESS IMPLICATIONS (~150 words) — Member 4
   - Summary of key findings
   - Actionable business recommendations
   - Limitations of the analysis
   - Future work suggestions

9. REFERENCES — Member 4
   - Harvard referencing format
   - Minimum 8-10 references
   - Include: Kaggle dataset, MongoDB docs, Spark docs,
     industry reports, academic sources on e-commerce analytics
```

### ✅ Gate Check P7-T1:

- [ ]  All sections assigned to specific members
- [ ]  Word count targets set for each section (totaling ~2500)
- [ ]  Each member knows their deadline (at least 1 day before submission)
- [ ]  Report template created with section headers

---

### P7-T2: References to Include

**Minimum references (Harvard format):**

```
REQUIRED REFERENCES:

1. Dataset citation:
   Kechinov, M. (2020) eCommerce Events History in Cosmetics Shop.
   Available at: <https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop>
   (Accessed: [date])

2. Apache Spark documentation:
   Apache Software Foundation (2024) Apache Spark Documentation.
   Available at: <https://spark.apache.org/docs/latest/> (Accessed: [date])

3. MongoDB documentation:
   MongoDB Inc. (2024) MongoDB Atlas Documentation.
   Available at: <https://www.mongodb.com/docs/atlas/> (Accessed: [date])

4. Databricks documentation:
   Databricks Inc. (2024) Databricks Documentation.
   Available at: <https://docs.databricks.com/> (Accessed: [date])

5. At least 2 academic/industry sources on e-commerce analytics
   (search Google Scholar for "e-commerce purchase prediction" or
   "customer behavior analytics")

6. At least 1 source on NoSQL databases vs relational databases

7. At least 1 source on the Big Data analytics value chain

8. At least 1 source on machine learning evaluation metrics
```

### ✅ Gate Check P7-T2:

- [ ]  Minimum 8 references gathered
- [ ]  All references in Harvard format
- [ ]  Dataset source properly cited
- [ ]  Tool documentation cited (Spark, MongoDB, Databricks)
- [ ]  At least 2 academic sources included

---

## PHASE 7 — COMPLETE VERIFICATION

- [ ]  All 4 members have written their sections
- [ ]  Total word count is approximately 2500 (±200)
- [ ]  Harvard referencing used throughout
- [ ]  Figures numbered and referenced in text
- [ ]  Report proofread for grammar and clarity
- [ ]  Report saved as .docx file ready for submission

---

## PHASE 8: Video Presentation (20–30 Minutes)

**Goal:** Record a Zoom presentation where all 4 members present their contributions.

**Time Estimate:** 2–3 hours (prep + recording)

---

### P8-T1: Presentation Structure

**Who:** All 4 members

```
VIDEO STRUCTURE (25 minutes target)

PART 1: Introduction & Dataset (Member 2) — 5-7 min
- Welcome and team introduction
- Dataset overview and industry context
- Business problem statement
- MongoDB demonstration (show Atlas screenshots, explain NoSQL concepts)

PART 2: Data Processing (You/Lead) — 7-8 min
- Show the Databricks notebook running
- Walk through data loading and cleaning
- Highlight key analytical findings
- Show code and explain what it does

PART 3: Visualization & Architecture (Member 3) — 5-7 min
- Show each visualization and explain insights
- Present the architecture diagram
- Explain each component's role

PART 4: Machine Learning & Conclusion (You/Lead + Member 4) — 6-8 min
- You: Walk through ML pipeline — feature engineering, model training
- You: Show comparison table and explain model selection
- Member 4: Summarize key findings and business recommendations
- Member 4: Discuss limitations and future work

CLOSING:
- Q&A prep (if applicable)
- Thank you slide
```

### P8-T2: Recording Guidelines

**Steps:**

1. Create a simple slide deck (Google Slides or PowerPoint) with key talking points
2. Each member should have Databricks notebook open for live demo sections
3. Use Zoom — one person shares screen, all cameras on
4. Record with Zoom's built-in recording
5. Upload recording to YouTube (unlisted) or university platform
6. Include the video link in the submission

**Tips:**

- Each member should rehearse their section at least once
- For the live demo: open the notebook in **Databricks web UI** (not VS Code) — it looks more professional and the module leader will recognize it as Databricks
- Keep slides minimal — mostly show the actual notebook running in the web UI
- Speak clearly and explain WHY, not just WHAT
- If showing code, zoom in so it's readable
- Run cells live during recording if possible — shows the module leader it actually works

### ✅ Gate Check P8:

- [ ]  All 4 members present and speak in the video
- [ ]  Video is 20-30 minutes long
- [ ]  Databricks notebook shown running during relevant sections
- [ ]  Key findings and comparison table shown
- [ ]  Video is clear (audio + visual) and uploadable
- [ ]  Video link obtained and ready for submission

---

## PHASE 9: Final Review & Submission

**Goal:** Quality check everything and submit before the deadline.

**Time Estimate:** 2 hours

**Deadline:** April 9, 2026, 16:00 SL Time

---

### P9-T1: Final Checklist

**SUBMISSION CHECKLIST — Every item must be checked:**

**Databricks Notebook:**

- [ ]  Both Python files sync to Databricks and are viewable as notebooks in the workspace
- [ ]  All outputs (tables, charts, model results) are visible when run from the Databricks web UI
- [ ]  Code is commented and readable
- [ ]  Main notebook exported as .dbc or .html from the Databricks web UI (Workspace → find the synced file → Export)
- [ ]  MongoDB notebook exported as .dbc or .html
- [ ]  Sharable link(s) created and tested (anyone with link can view)
- [ ]  ⚠️ EXPORT BEFORE TRIAL EXPIRES (Apr 11) — do this by Apr 8 at the latest

**Written Report:**

- [ ]  ~2500 words (±200)
- [ ]  All 6 steps covered (dataset, MongoDB, Spark, visualization, ML, architecture)
- [ ]  Harvard referencing with 8+ references
- [ ]  Figures numbered and referenced in text
- [ ]  Architecture diagram included
- [ ]  MongoDB screenshots included
- [ ]  ML comparison table included
- [ ]  Proofread for grammar and spelling
- [ ]  Saved as .docx

**Video Presentation:**

- [ ]  20-30 minutes
- [ ]  All 4 members present and speak equally (~5-7 min each)
- [ ]  Databricks notebook shown running
- [ ]  Key findings presented clearly
- [ ]  Video link working and accessible

**Submission:**

- [ ]  All files uploaded to DLE portal
- [ ]  Submitted BEFORE 16:00 SL time on April 9, 2026
- [ ]  Confirmation of submission received

### P9-T2: Last-Minute Quality Boosters (If Time Allows)

These small additions can push you from 75% to 80%+:

- [ ]  Add a "Limitations" section in the report (e.g., dataset only covers 5 months, class imbalance affects model performance)
- [ ]  Add a "Future Work" paragraph (e.g., real-time streaming with Kafka, deep learning models, A/B testing integration)
- [ ]  Include Spark execution plan screenshot (shows distributed computing awareness)
- [ ]  Add confusion matrix visualization for the best ML model
- [ ]  Mention how descriptive, predictive, AND prescriptive analytics are demonstrated (directly maps to module learning outcomes)
- [ ]  Reference the Big Data Analytics Value Chain from Lesson 1 explicitly in the report introduction
- [ ]  Mention Databricks Serverless compute in the architecture section — shows awareness of modern cloud infrastructure

---

## PHASE 9 — FINAL VERIFICATION

- [ ]  ALL deliverables complete (notebook + report + video)
- [ ]  ALL 6 assessment steps covered with required content
- [ ]  Submitted before deadline with confirmation
- [ ]  Video link included in submission

---

## APPENDIX A: VS Code + Databricks Serverless Survival Tips

1. **Save frequently** — Ctrl+S saves AND syncs to Databricks automatically
2. **Sync broken?** — Check the Databricks extension panel in VS Code sidebar. Re-authenticate if needed.
3. **Code not running?** — Serverless compute may take 10-30 seconds to cold-start on first run. Subsequent runs are instant.
4. **Out of memory?** — Use `.sample(0.1)` to work on 10% of data during development, then run full data for final outputs
5. **Lost outputs?** — Re-run all cells from top in VS Code, or open the notebook in the Databricks web UI and "Run All"
6. **Export notebook from web UI:** Go to Databricks workspace → find the synced Python file → click ⋮ menu → Export → DBC Archive or HTML
7. **Get sharable link from web UI:** Right-click the notebook → Share → Get Link
8. **Cell separator:** Always use exactly `# COMMAND ----------` (with spaces and 10 hyphens)
9. **Markdown in cells:** Every line must start with `# MAGIC %md` (first line) or `# MAGIC`  (subsequent lines)
10. **Trial expiry warning:** Export ALL notebooks by April 8. Once trial expires, you lose workspace access.
11. **Premium advantages you have:** Serverless = faster compute, no cluster timeouts, better memory. Your ML training will be faster than Community Edition.

## APPENDIX B: VS Code + AI Coding Agent Workflow

**This is your primary development workflow.** You write code locally with AI assistance, and it runs on Databricks serverless.

### How the Pipeline Works

```
You type code in VS Code
        ↓
AI agent (Copilot/Claude Code) suggests/generates code
        ↓
You save the .py file (Ctrl+S)
        ↓
Databricks Extension auto-syncs to workspace (< 1 second)
        ↓
You click Run Cell → executes on Databricks Serverless
        ↓
Output appears in VS Code output panel
```

### Writing Code for AI Agents

When prompting Copilot or Claude Code, include this context:

```
I'm writing a PySpark notebook for Databricks using the VS Code Databricks Extension.
- Use `# COMMAND ----------` between cells (exactly 10 hyphens)
- For markdown: use `# MAGIC %md` on first line, `# MAGIC ` prefix on all subsequent lines
- `spark` session is pre-created — do NOT create SparkSession
- `display()` works for rich table output
- `dbutils` is available for file operations
- Data is at DATA_PATH = "/Volumes/workspace/default/cosmetics_data" (or "/FileStore/tables" if DBFS was used)
- Libraries: PySpark, matplotlib, seaborn are pre-installed
- For pip installs: use `# MAGIC %pip install "package"`
```

### Cell Format Quick Reference

```python
# Python code cell:
# COMMAND ----------
df = spark.read.csv(f"{DATA_PATH}/2019-Oct.csv", header=True, inferSchema=True)
print(f"Rows: {df.count()}")

# COMMAND ----------

# Markdown cell:
# COMMAND ----------
# MAGIC %md
# MAGIC ## Section Title
# MAGIC This is a description of what follows.

# COMMAND ----------

# Pip install cell:
# COMMAND ----------
# MAGIC %pip install "pymongo[srv]"

# COMMAND ----------
```

### Exporting for Submission

Your `.py` files synced to Databricks ARE notebooks. To export:

1. Open Databricks web UI in browser
2. Navigate to Workspace → Users → your email → look for the synced files
3. Click the file → it opens as a notebook with all your cells
4. **Run All** to generate fresh outputs (important — exported notebooks should have visible outputs)
5. Click ⋮ menu → **Export** → **DBC Archive** or **HTML**
6. The exported file is your submission artifact

### Development Sampling Strategy

When developing, use 10% of data to iterate fast:

```python
# During development — fast iteration
df_dev = df.sample(0.1, seed=42)

# For final submission — use full dataset
df_full = df
```

## APPENDIX C: Common PySpark Code Snippets

```python
# Import everything you'll need
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator, ClusteringEvaluator
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Load CSV
df = spark.read.csv("/Volumes/workspace/default/cosmetics_data/file.csv", header=True, inferSchema=True)

# Basic operations
df.count()                          # Row count
df.printSchema()                    # Column types
df.describe().show()                # Summary stats
df.select("col").distinct().show()  # Unique values
df.groupBy("col").count().show()    # Group and count
df.filter(F.col("col") == "value")  # Filter rows
df.orderBy("col", ascending=False)  # Sort

# Convert Spark DataFrame to Pandas (for visualization — use on small/aggregated data only)
pdf = df.limit(10000).toPandas()
```

## APPENDIX D: Key Marks Differentiators

**What separates 65% from 80%+:**

| Area | 65% (Average) | 80%+ (Excellent) |
| --- | --- | --- |
| Step 1 | Basic dataset description | Industry context with real-world relevance, clear problem definition |
| Step 2 | MongoDB screenshots only | Screenshots + explanation of schema flexibility with dataset-specific examples |
| Step 3 | 3-4 basic aggregations | 8+ analyses with business interpretations for each |
| Step 4 | 3 basic charts | 5 diverse charts with trend analysis and actionable insights |
| Step 5 | 1 model trained | 3 models compared in a table, best model justified, feature importance shown |
| Step 6 | Basic flow diagram | Professional diagram with 6+ components and clear role explanations |
| Report | Describes what was done | Explains why decisions were made and what insights mean for the business |
| Video | Reading from slides | Live notebook demo with confident explanation of code and results |

## APPENDIX E: Final Export Checklist (CRITICAL — Premium Trial)

**Your Premium Trial expires ~April 11. You MUST export everything before then.**

Do this on **April 8 at the latest:**

1. Open Databricks web UI: `https://dbc-1481bae3-1700.cloud.databricks.com`
2. Go to Workspace → Users → your email → find both synced Python files
3. For each file:
    - Open it (it appears as a notebook in the web UI)
    - Click **Run All** to regenerate all outputs
    - Wait for all cells to complete
    - Click ⋮ menu → **Export** → **DBC Archive** AND **HTML** (export both formats)
    - Save to your local machine
4. Also create a **Sharable Link**:
    - Right-click the notebook → **Share** → **Get Link**
    - Set permissions to "Can View"
    - Copy and save the link — include it in your report submission
5. Download any saved charts/images from the notebook output
6. **Verify** the exported HTML opens correctly in a browser and shows all outputs

**If you forget this step, you lose access to your entire project after the trial expires.**