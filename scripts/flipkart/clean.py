import sys, argparse
from pyspark.sql.functions import col
sys.path.append("/opt/airflow/scripts")
from common.spark_connector import get_spark_session, return_xcom

def run(input_path):
    VENDOR = "flipkart"
    spark = get_spark_session(f"{VENDOR}_clean")
    df = spark.read.option("header", "true").csv(input_path)
    
    # Flipkart Logic: Amount > 10
    clean_df = df.filter(col("amount").cast("float") > 10)
    
    out_path = f"/opt/airflow/data/lake/silver/cleansed/{VENDOR}"
    clean_df.write.format("delta").mode("overwrite").save(out_path)
    return_xcom(out_path, clean_df.count())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    run(parser.parse_args().input_path)