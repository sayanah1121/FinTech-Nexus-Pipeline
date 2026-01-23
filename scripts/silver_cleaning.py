import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_silver_cleaning(vendor):
    print(f" Starting Silver Cleaning for Vendor: {vendor}...")
    
    spark = SparkSession.builder.appName(f"Silver_Clean_{vendor}") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Paths
    bronze_path = "/opt/airflow/data/lake/bronze"
    # Save to specific subfolder (Partitioning by directory)
    output_path = f"/opt/airflow/data/lake/silver/cleansed/{vendor}"

    df = spark.read.format("delta").load(bronze_path)

    # Filter for specific vendor AND clean data
    clean_df = df.filter(
        (col("vendor") == vendor) & 
        (col("amount").cast("float") > 0) & 
        (col("user_id").isNotNull())
    )

    clean_df.write.format("delta").mode("overwrite").save(output_path)
    print(f" {vendor} Cleaning Complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--vendor", required=True)
    args = parser.parse_args()
    run_silver_cleaning(args.vendor)