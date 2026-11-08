from pyspark.sql import SparkSession
import argparse
import sys

# Argument parser configuration
parser = argparse.ArgumentParser()
parser.add_argument("--catalog", type=str, default="dev", help="Target Unity Catalog Name")
args, unknown = parser.parse_known_args()

target_catalog = args.catalog
print(f"Executing pipeline on Catalog: {target_catalog}")

# Create Spark session
spark = SparkSession.builder.appName("basic").getOrCreate()
# Mandatory Columns list
mandatory_columns = [
    "Patient_ID",
    "Age",
    "Gender",
    "Condition",
    "Procedure",
    "Cost",
    "Length_of_Stay",
    "Readmission",
    "Outcome",
    "Satisfaction",
    "_rescued_data",
    "Ingestion_Time",
    "Source_file_path",
]
# Existing columns list
df_bronze = spark.read.table("dev.cro_schema.hospital_data_bronze")
existing_columns = df_bronze.columns

# --- CASE 1: Check for Missing Mandatory Columns ---
missing_cols = [col for col in mandatory_columns if col not in existing_columns]
if len(missing_cols) > 0:
    raise Exception(
        f"🚨 CRITICAL ERROR: Mandatory columns {missing_cols} are missing from the source file! Pipeline failed."
    )

# --- CASE 2: Check for Extra / Optional Columns (_rescued_data) ---

rescue_check_df = spark.sql("""
                            select count(*) as rescued_data_count
                            from dev.cro_schema.hospital_data_bronze
                            where _rescued_data is not null
                            """)
rescued_count = rescue_check_df.collect()[0]["rescued_data_count"]
if rescued_count > 0:
    print(f"⚠️ WARNING: {rescued_count} rows contain extra/unexpected columns in _rescued_data.")

else:
    print("✅ Schema Validation Passed: All mandatory columns present, no extra columns found.")
