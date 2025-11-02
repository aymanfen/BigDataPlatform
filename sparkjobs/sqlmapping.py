import sys
from pyspark.sql import SparkSession
import os

# --- Validate CLI args ---
if len(sys.argv) < 2:
    print("Usage: spark-submit sql_mapper.py <Topic>")
    sys.exit(1)

topic = sys.argv[1]

# --- Paths ---
input_path = f"/opt/sparkjobs/{topic}.csv"
sql_path = f"/opt/sparkjobs/sql/{topic}.sql"
output_path = f"/opt/sparkjobs/sqloutput/{topic}/"

# --- Check if input and SQL query exist ---
if not os.path.exists(input_path):
    print(f"[ERROR] Input file '{input_path}' not found.")
    sys.exit(1)

if not os.path.exists(sql_path):
    print(f"[ERROR] SQL file '{sql_path}' not found.")
    sys.exit(1)

# --- Initialize Spark ---
spark = SparkSession.builder.appName(f"{topic}SQLMapping").getOrCreate()

# --- Load CSV Data ---
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

# --- Register Temp View ---
df.createOrReplaceTempView(topic)

# --- Load SQL Query ---
with open(sql_path, "r") as f:
    query = f.read()

# --- Execute SQL ---
try:
    result_df = spark.sql(query)
except Exception as e:
    print(f"[ERROR] Failed to execute SQL: {e}")
    spark.stop()
    sys.exit(1)

# --- Save Result ---
result_df.write.mode("overwrite").option("header", "true").csv(output_path)

print(f"[SUCCESS] Output written to {output_path}")

# --- Stop Spark ---
spark.stop()
