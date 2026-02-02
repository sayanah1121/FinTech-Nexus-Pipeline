import sys, argparse
from pyspark.sql.functions import col, sha2, lit
sys.path.append("/opt/airflow/scripts")
from common.spark_connector import get_spark_session, return_xcom

def run(input_path):
    VENDOR = "amazon"
    print(f" [{VENDOR}] Enriching...", file=sys.stderr)
    
    spark = get_spark_session(f"{VENDOR}_enrich")
    txns = spark.read.format("delta").load(input_path)
    cibil = spark.read.option("header", "true").csv("/opt/airflow/data/reference/user_cibil_master.csv")

    enriched = txns.join(cibil, "user_id", "left").withColumn("user_id_hash", sha2(col("user_id"), 256)).drop("user_id")
    
    out_path = f"/opt/airflow/data/lake/silver/enriched/{VENDOR}"
    enriched.write.format("delta").mode("overwrite").save(out_path)
    return_xcom(out_path, enriched.count())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    run(parser.parse_args().input_path)