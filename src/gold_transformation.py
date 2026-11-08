from pyspark.sql import SparkSession
import logging
import argparse
import sys
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# Argument parser configuration
parser = argparse.ArgumentParser()
parser.add_argument("--catalog", type=str, default="dev_catalog", help="Target Unity Catalog Name")
args, unknown = parser.parse_known_args()

target_catalog = args.catalog
print(f"Executing pipeline on Catalog: {target_catalog}")

# Create logging and spark object
spark = SparkSession.builder.appName("basics").getOrCreate()
logging.basicConfig(level=logging.INFO)
Logger = logging.getLogger("CRO_Express_Edge_gold")

try:
    Logger.info("Reading silver table")
    # Dynamic silver table path
    silver_table_name = f"{target_catalog}.cro_schema.cleaned_hospital_patients"
    df_raw = spark.read.table(silver_table_name)

    df_gold = (
        df_raw.filter("is_current = true")
        .groupBy("Condition", "Procedure")
        .agg(
            F.count("Patient_ID").alias("Patient_Count"),
            F.avg("Cost").alias("Avg_Cost"),
            F.avg("Length_of_Stay").alias("Avg_Length_of_Stay"),
        )
        .orderBy("Avg_Cost", ascending=False)
    )

    Logger.info("Writing to gold table")
    # Dynamic gold table path
    gold_table_name = f"{target_catalog}.cro_schema.gold_hospital_patients"
    df_gold.write.format("delta").mode("overwrite").saveAsTable(gold_table_name)
    Logger.info(f"Data written to gold table {gold_table_name}")

except AnalysisException as ae:
    Logger.error(f"Pyspark Analysis Error occured: {str(ae)}")
    raise ae
except Exception as e:
    Logger.error(f"An unexpected error occurred in the pipeline: {str(e)}")
    raise e
