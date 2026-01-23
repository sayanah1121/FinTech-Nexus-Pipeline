import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2

def run_silver_enrichment(vendor):
    print(f" Starting Silver Enrichment for Vendor: {vendor}...")
    
    spark = SparkSession.builder.appName(f"Silver_Enrich_{vendor}") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Read from vendor specific Clean folder
    clean_path = f"/opt/airflow/data/lake/silver/cleansed/{vendor}"
    cibil_source = "/opt/airflow/data/reference/user_cibil_master.csv"
    output_path = f"/opt/airflow/data/lake/silver/enriched/{vendor}"

    txns_df = spark.read.format("delta").load(clean_path)
    cibil_df = spark.read.option("header", "true").csv(cibil_source)

    enriched_df = txns_df.join(cibil_df, "user_id", "left") \
        .withColumn("user_id_hash", sha2(col("user_id"), 256)) \
        .drop("user_id")

    enriched_df.write.format("delta").mode("overwrite").save(output_path)
    print(f" {vendor} Enrichment Complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--vendor", required=True)
    args = parser.parse_args()
    run_silver_enrichment(args.vendor)