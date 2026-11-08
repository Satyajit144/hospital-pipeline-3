import logging
import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.utils import AnalysisException

# Argument parser configuration
parser = argparse.ArgumentParser()
parser.add_argument("--catalog", type=str, default="dev_catalog", help="Target Unity Catalog Name")
args, unknown = parser.parse_known_args()

target_catalog = args.catalog
print(f"Executing pipeline on Catalog: {target_catalog}")

# Create Spark session
spark = SparkSession.builder.appName("basic").getOrCreate()

logging.basicConfig(level=logging.INFO)
Logger = logging.getLogger("CRO_Express_Edge_Schema_Validation")

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

try:
    # 1. Dynamic catalog use karein table read karne ke liye
    table_name = f"{target_catalog}.cro_schema.hospital_data_bronze"
    df_bronze = spark.read.table(table_name)
    existing_columns = df_bronze.columns

    # --- CASE 1: Check for Missing Mandatory Columns ---
    missing_cols = [col for col in mandatory_columns if col not in existing_columns]
    if len(missing_cols) > 0:
        raise Exception(
            f"🚨 CRITICAL ERROR: Mandatory columns {missing_cols} are missing from the source file! Pipeline failed."
        )

    # --- CASE 2: Check for Extra / Optional Columns (_rescued_data) ---
    rescue_check_df = spark.sql(f"""
                                select count(*) as rescued_data_count
                                from {target_catalog}.cro_schema.hospital_data_bronze
                                where _rescued_data is not null
                                """)
    rescued_count = rescue_check_df.collect()[0]["rescued_data_count"]

    if rescued_count > 0:
        print(f"⚠️ WARNING: {rescued_count} rows contain extra/unexpected columns in _rescued_data.")
    else:
        print("✅ Schema Validation Passed: All mandatory columns present, no extra columns found.")

except AnalysisException as ae:
    Logger.error(f"PySpark Analysis Error occurred: {str(ae)}")
    raise ae
except Exception as e:
    Logger.error(f"An unexpected error occurred in pipeline: {str(e)}")
    raise e
