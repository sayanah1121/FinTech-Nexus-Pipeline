from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_fraud_detector():
    print(" Starting Gold Layer: Fraud Detection...")
    
    spark = SparkSession.builder.appName("FinGuard_Gold_Fraud") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Docker Paths
    input_path = "/opt/airflow/data/lake/silver/enriched"
    output_path = "/opt/airflow/data/lake/gold/fraud_alerts"

    df = spark.read.format("delta").load(input_path)
    
    # Fraud Logic
    fraud_df = df.filter(
        ((col("payment_mode") == "UPI") & (col("amount").cast("float") > 50000)) | 
        ((col("cibil_score").cast("int") < 600) & (col("amount").cast("float") > 20000) & (col("payment_mode") == "Credit Card"))
    )

    fraud_df.write.format("delta").mode("overwrite").save(output_path)
    print(f" Fraud Analysis Complete.")

if __name__ == "__main__":
    run_fraud_detector()