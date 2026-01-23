import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType

def run_reward_engine():
    print(" Starting Gold Layer: Reward Engine...")
    
    spark = SparkSession.builder.appName("FinGuard_Gold_Rewards") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Docker Paths
    config_path = "/opt/airflow/config/business_rules.json"
    input_path = "/opt/airflow/data/lake/silver/enriched"
    output_path = "/opt/airflow/data/lake/gold/banking_rewards"

    with open(config_path, "r") as f:
        rules = json.load(f)
    
    rules_bc = spark.sparkContext.broadcast(rules)

    def calculate_points(amount, payment_mode, cibil_score, vendor):
        if not amount or not cibil_score: return 0.0
        
        r = rules_bc.value["reward_multipliers"]
        
        mode_rules = r["payment_modes"].get(payment_mode, r["payment_modes"]["NetBanking"])
        multiplier = mode_rules["base_multiplier"]
        
        cibil = int(cibil_score)
        if cibil >= 800: bonus = mode_rules["cibil_bonus"].get("excellent", {}).get("extra", 0)
        elif cibil >= 700: bonus = mode_rules["cibil_bonus"].get("good", {}).get("extra", 0)
        else: bonus = 0
        
        boost = r["merchant_category_boost"].get(vendor, 1.0)
        return float(amount * (multiplier + bonus) * boost)

    points_udf = udf(calculate_points, FloatType())

    df = spark.read.format("delta").load(input_path)
    
    # Cast types before UDF
    df_typed = df.withColumn("cibil_score", col("cibil_score").cast("integer")) \
                 .withColumn("amount", col("amount").cast("float"))
    
    gold_df = df_typed.withColumn("earned_points", points_udf(col("amount"), col("payment_mode"), col("cibil_score"), col("vendor")))

    gold_df.write.format("delta").mode("overwrite").save(output_path)
    print(f" Reward Calculation Complete.")

if __name__ == "__main__":
    run_reward_engine()