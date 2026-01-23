from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, col

def run_vendor_stats():
    print(" Starting Gold Layer: Vendor Settlement Stats...")
    
    spark = SparkSession.builder.appName("FinGuard_Gold_Vendor") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Docker Paths
    input_path = "/opt/airflow/data/lake/silver/enriched"
    output_path = "/opt/airflow/data/lake/gold/vendor_stats"

    df = spark.read.format("delta").load(input_path)

    stats_df = df.groupBy("vendor", "payment_mode").agg(
        _sum("amount").alias("total_volume"),
        count("txn_id").alias("txn_count")
    )

    stats_df.write.format("delta").mode("overwrite").save(output_path)
    print(f" Vendor Stats Complete.")

if __name__ == "__main__":
    run_vendor_stats()