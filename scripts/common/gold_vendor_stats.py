from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, col, current_date, lit, round

def run_vendor_stats():
    print(" Starting Gold Layer: Vendor Settlement Stats (Global View)...")
    
    spark = SparkSession.builder.appName("FinGuard_Gold_Vendor") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # --- FAN-IN MERGE ---
    base_path = "/opt/airflow/data/lake/silver/enriched"
    try:
        df_amz = spark.read.format("delta").load(f"{base_path}/amazon")
        df_fk = spark.read.format("delta").load(f"{base_path}/flipkart")
        df_pp = spark.read.format("delta").load(f"{base_path}/paypal")
        df_bh = spark.read.format("delta").load(f"{base_path}/blackhawk")
        
        df = df_amz.unionByName(df_fk, allowMissingColumns=True) \
                   .unionByName(df_pp, allowMissingColumns=True) \
                   .unionByName(df_bh, allowMissingColumns=True)
    except Exception as e:
        print(f" Error merging streams for Stats: {e}")
        return

    output_path = "/opt/airflow/data/lake/gold/vendor_stats"

    # Aggregation
    stats_df = df.groupBy("vendor", "payment_mode").agg(
        _sum("amount").alias("total_volume"),
        count("txn_id").alias("txn_count")
    )
    
    # Add Professional Columns
    final_df = stats_df.withColumn("report_date", current_date()) \
        .withColumn("avg_ticket_size", round(col("total_volume") / col("txn_count"), 2)) \
        .withColumn("settlement_status", lit("PENDING_CLEARANCE")) \
        .select("report_date", "vendor", "payment_mode", "total_volume", "txn_count", "avg_ticket_size", "settlement_status")

    final_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
    print(f" Vendor Stats Aggregation Complete.")

if __name__ == "__main__":
    run_vendor_stats()