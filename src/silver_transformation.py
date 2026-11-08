from pyspark.sql import SparkSession
import logging
import argparse
import sys
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from delta.tables import DeltaTable
from pyspark.sql.types import TimestampType

# Argument parser configuration
parser = argparse.ArgumentParser()
parser.add_argument("--catalog", type=str, default="dev", help="Target Unity Catalog Name")
args, unknown = parser.parse_known_args()

target_catalog = args.catalog
print(f"Executing pipeline on Catalog: {target_catalog}")

# Create logging and spark object
spark = SparkSession.builder.appName("basics").getOrCreate()
logging.basicConfig(level=logging.INFO)
Logger = logging.getLogger("CRO_Express_Edge_Silver")

try:
    Logger.info("Starting Ingesting Bronze file to Silver Layer")

    schema = StructType(
        [
            StructField("Patient_ID", IntegerType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Gender", StringType(), True),
            StructField("Condition", StringType(), True),
            StructField("Procedure", StringType(), True),
            StructField("Cost", DoubleType(), True),
            StructField("Length_of_Stay", IntegerType(), True),
            StructField("Readmission", StringType(), True),
            StructField("Outcome", StringType(), True),
            StructField("Satisfaction", IntegerType(), True),
        ]
    )

    Logger.info("Enforcing Business Schema and ingesting data from Bronze")
    df_Silver_raw = spark.read.schema(schema).table("dev.cro_schema.hospital_data_bronze")

    # 1. Quality Rules Definition
    is_valid_rule = (F.col("Patient_ID").isNotNull()) & (F.col("Age").between(1, 120))

    # 2. Data Splitting
    df_Silver_Clean_data = df_Silver_raw.filter(is_valid_rule)

    # Add audit columns to invalid data for easier debugging
    df_Invalid_data = df_Silver_raw.filter(~is_valid_rule).withColumn("rejected_at", F.current_timestamp())

    # 3. Data Transformations
    df_Silver_data = df_Silver_Clean_data.withColumns(
        {
            "Condition": F.initcap(F.trim(F.col("Condition"))),
            "Procedure": F.initcap(F.trim(F.col("Procedure"))),
            "Cost": F.col("Cost").cast("double"),
            "effective_start_date": F.current_timestamp(),
            "effective_end_date": F.lit(None).cast(TimestampType()),
            "is_current": F.lit(True),
        }
    )

    # 4. Write to Silver Table (SCD Type 2)
    if not spark.catalog.tableExists("dev.cro_schema.cleaned_hospital_patients"):
        df_Silver_data.write.format("delta").mode("overwrite").option("mergeSchema", "true").option(
            "overwriteSchema", "true"
        ).saveAsTable("dev.cro_schema.cleaned_hospital_patients")
    else:
        df_target = DeltaTable.forName(spark, "dev.cro_schema.cleaned_hospital_patients")
        df_target.alias("t").merge(
            df_Silver_data.alias("s"), "t.Patient_ID = s.Patient_ID AND t.is_current = true"
        ).whenMatchedUpdate(
            condition="s.Age <> t.Age OR s.Condition <> t.Condition OR s.Readmission <> t.Readmission",
            set={"is_current": F.lit(False), "effective_end_date": F.current_timestamp()},
        ).whenNotMatchedInsertAll().execute()

    Logger.info("Data Successfully Ingested to Silver Layer Table")

    # 5. Write Quarantine Data (Append Mode for Audit Trail)
    df_Invalid_data.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        "dev.cro_schema.invalid_hospital_patients"
    )
    Logger.info("Quarantine Data Successfully Logged")

except AnalysisException as ae:
    Logger.error(f"PySpark Analysis Error occurred: {str(ae)}")
    raise ae
except Exception as e:
    Logger.error(f"An unexpected error occurred in the pipeline: {str(e)}")
    raise e
