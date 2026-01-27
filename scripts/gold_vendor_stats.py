from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, col, current_date, lit, round

def run_vendor_stats():
    print(" Starting Gold Layer: Vendor Settlement Stats...")
    
    spark = SparkSession.builder.appName("FinGuard_Gold_Vendor") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    input_path = "/opt/airflow/data/lake/silver/enriched"
    output_path = "/opt/airflow/data/lake/gold/vendor_stats"

    df = spark.read.format("delta").load(input_path)

    # Aggregation
    stats_df = df.groupBy("vendor", "payment_mode").agg(
        _sum("amount").alias("total_volume"),
        count("txn_id").alias("txn_count")
    )
    
    # Add Professional Columns
    final_df = stats_df.withColumn("report_date", current_date()) \
        .withColumn("avg_ticket_size", round(col("total_volume") / col("txn_count"), 2)) \
        .withColumn("settlement_status", lit("PENDING")) \
        .select("report_date", "vendor", "payment_mode", "total_volume", "txn_count", "avg_ticket_size", "settlement_status")

    final_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
    print(f" Vendor Stats Complete.")

if __name__ == "__main__":
    run_vendor_stats()