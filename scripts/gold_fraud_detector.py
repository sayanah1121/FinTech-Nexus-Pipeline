from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, current_timestamp, expr
import uuid

def run_fraud_detector():
    print(" Starting Gold Layer: Risk Scoring Engine (4 Levels)...")
    
    spark = SparkSession.builder.appName("FinGuard_Gold_Risk_Scoring") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    input_path = "/opt/airflow/data/lake/silver/enriched"
    # We continue to save to 'fraud_alerts' but now it contains ALL scored transactions
    output_path = "/opt/airflow/data/lake/gold/fraud_alerts"

    df = spark.read.format("delta").load(input_path)
    
    # --- 4-TIER RISK LOGIC ---
    
    # 1. CRITICAL: High Value UPI (The "Red" Zone)
    cond_critical = (col("payment_mode") == "UPI") & (col("amount").cast("float") > 30000)
    
    # 2. HIGH: Risky Credit Usage (The "Orange" Zone)
    cond_high = (col("cibil_score").cast("int") < 600) & (col("amount").cast("float") > 15000) & (col("payment_mode") == "Credit Card")
    
    # 3. MODERATE: Just expensive transactions (The "Yellow" Zone)
    # Any transaction over 10,000 that wasn't already caught above
    cond_moderate = (col("amount").cast("float") > 10000)

    # Apply Logic Chain
    scored_df = df.withColumn("risk_severity", 
                              when(cond_critical, "CRITICAL")
                              .when(cond_high, "HIGH")
                              .when(cond_moderate, "MODERATE")
                              .otherwise("LOW RISK")) \
                  .withColumn("risk_reason",
                              when(cond_critical, "Suspiciously High UPI")
                              .when(cond_high, "Credit Risk Violation")
                              .when(cond_moderate, "High Value Transaction")
                              .otherwise("Normal Activity")) \
                  .withColumn("is_fraud", 
                              when(cond_critical | cond_high, True).otherwise(False))

    # Add Standard Columns
    final_df = scored_df.withColumn("alert_id", expr("uuid()")) \
        .withColumnRenamed("txn_id", "transaction_id") \
        .withColumn("investigation_status", 
                    when(col("is_fraud") == True, "OPEN").otherwise("CLOSED")) \
        .withColumn("detected_at", current_timestamp()) \
        .select(
            "alert_id", "transaction_id", "txn_date", "amount", 
            "vendor", "payment_mode", "risk_severity", 
            "risk_reason", "investigation_status", "is_fraud", "detected_at"
        )

    final_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
    print(f" Risk Scoring Complete. Total Transactions Processed: {final_df.count()}")

if __name__ == "__main__":
    run_fraud_detector()