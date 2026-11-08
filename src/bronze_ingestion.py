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

spark = SparkSession.builder.appName("Basics").getOrCreate()

logging.basicConfig(level=logging.INFO)
Logger = logging.getLogger("CRO_Express_Edge_Bronze_Pipeline")

try:
    Logger.info("Starting Data Ingestion from Staging to Bronze")

    # 1. Dynamic catalog use karein schemaLocation mein
    df_source_data = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option(
            "cloudFiles.schemaLocation",
            f"/Volumes/{target_catalog}/cro_schema/landing_container/_schemas/hospital_bronze",
        )
        .load(f"/Volumes/{target_catalog}/cro_schema/landing_container/")
    )

    df_bronze_data = df_source_data.withColumns(
        {"Ingestion_Time": f.current_timestamp(), "Source_file_path": f.col("_metadata.file_path")}
    )

    # 2. Dynamic catalog use karein checkpointLocation aur toTable mein
    query = (
        df_bronze_data.writeStream.format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation", f"/Volumes/{target_catalog}/cro_schema/landing_container/_checkpoints/hospital_bronze"
        )
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{target_catalog}.cro_schema.hospital_data_bronze")
    )

    query.awaitTermination()
    Logger.info("Batch Ingestion to Bronze Layer completed successfully.")

except AnalysisException as ae:
    Logger.error(f"PySpark Analysis Error occurred: {str(ae)}")
    raise ae
except Exception as e:
    Logger.error(f"An unexpected error occurred in pipeline: {str(e)}")
    raise e
