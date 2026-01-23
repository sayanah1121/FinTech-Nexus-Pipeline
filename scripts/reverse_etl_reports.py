import sys
import argparse
import os
import shutil
from pyspark.sql import SparkSession

def run_reverse_etl(vendor):
    print(f" Generating Settlement Report for: {vendor}...")
    
    spark = SparkSession.builder.appName(f"Report_{vendor}") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Read the consolidated Vendor Stats Gold Table
    input_path = "/opt/airflow/data/lake/gold/vendor_stats"
    base_output = "/opt/airflow/data/settlements"

    df = spark.read.format("delta").load(input_path)
    
    # Filter & Write
    vendor_df = df.filter(df.vendor == vendor)
    vendor_path = f"{base_output}/{vendor.lower()}_settlement"
    
    vendor_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(vendor_path)
    
    # Rename
    for filename in os.listdir(vendor_path):
        if filename.endswith(".csv"):
            clean_name = f"{base_output}/{vendor.lower()}_final_report.csv"
            shutil.move(os.path.join(vendor_path, filename), clean_name)
            print(f" Report Created: {clean_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--vendor", required=True)
    args = parser.parse_args()
    run_reverse_etl(args.vendor)