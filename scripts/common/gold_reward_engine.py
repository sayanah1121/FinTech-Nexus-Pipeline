import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, current_timestamp, expr, when, lit
from pyspark.sql.types import FloatType

def run_reward_engine():
    print(" Starting Gold Layer: Reward Engine (Fan-In Mode)...")
    
    spark = SparkSession.builder.appName("FinGuard_Gold_Rewards") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    config_path = "/opt/airflow/config/business_rules.json"
    output_path = "/opt/airflow/data/lake/gold/banking_rewards"

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
        print(f" Error merging streams for Rewards: {e}")
        return

    # Load Business Rules
    try:
        with open(config_path, "r") as f:
            rules = json.load(f)
        rules_bc = spark.sparkContext.broadcast(rules)
    except:
        # Fallback if config is missing (Safe default)
        rules = {"reward_multipliers": {"payment_modes": {"NetBanking": {"base_multiplier": 1.0}}, "merchant_category_boost": {}}}
        rules_bc = spark.sparkContext.broadcast(rules)

    def calculate_points(amount, payment_mode, cibil_score, vendor):
        if not amount: return 0.0
        r = rules_bc.value["reward_multipliers"]
        
        # Handle cases where payment mode might be new/unknown
        mode_rules = r["payment_modes"].get(payment_mode, r["payment_modes"].get("NetBanking", {"base_multiplier": 1.0}))
        multiplier = mode_rules.get("base_multiplier", 1.0)
        
        cibil = int(cibil_score) if cibil_score else 300
        
        bonus = 0
        if cibil >= 800: bonus = mode_rules.get("cibil_bonus", {}).get("excellent", {}).get("extra", 0)
        elif cibil >= 700: bonus = mode_rules.get("cibil_bonus", {}).get("good", {}).get("extra", 0)
        
        boost = r["merchant_category_boost"].get(vendor, 1.0)
        return float(amount * (multiplier + bonus) * boost)

    points_udf = udf(calculate_points, FloatType())
    
    df_typed = df.withColumn("cibil_score", col("cibil_score").cast("integer")) \
                 .withColumn("amount", col("amount").cast("float"))
    
    # Calculate Points
    df_scored = df_typed.withColumn("earned_points", points_udf(col("amount"), col("payment_mode"), col("cibil_score"), col("vendor")))
    
    # Add Professional Columns
    final_df = df_scored.withColumn("reward_id", expr("uuid()")) \
        .withColumnRenamed("txn_id", "transaction_id") \
        .withColumnRenamed("user_id", "user_id_hash") \
        .withColumn("reward_tier", 
                    when(col("cibil_score") > 800, "Platinum")
                    .when(col("cibil_score") > 700, "Gold")
                    .otherwise("Standard")) \
        .withColumn("processed_at", current_timestamp()) \
        .select(
            "reward_id", "user_id_hash", "transaction_id", "vendor", 
            "payment_mode", "earned_points", "reward_tier", "processed_at"
        )

    final_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
    print(f" Reward Calculation Complete.")

if __name__ == "__main__":
    run_reward_engine()