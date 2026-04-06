# COMMAND ----------
try:
    spark  # type: ignore[name-defined]
except NameError:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()

print("Spark session initialized!")

# COMMAND ----------
print("Hello from Databricks Serverless!")

# COMMAND ----------
print(f"Spark version: {spark.version}")

# COMMAND ----------
from databricks.sdk.runtime import dbutils, display

DATA_PATH = "/Volumes/workspace/default/cosmetics_data"

files = dbutils.fs.ls(DATA_PATH)
display(files)
# COMMAND ----------
