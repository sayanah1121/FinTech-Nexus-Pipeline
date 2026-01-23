from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

def ingest_to_bronze():
    print(" Starting Bronze Ingestion (CSV -> Delta)...")
    
    spark = SparkSession.builder \
        .appName("FinGuard_Bronze_Ingest") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Docker Paths
    raw_source = "/opt/airflow/data/raw/*/*.csv"
    bronze_path = "/opt/airflow/data/lake/bronze"

    print("   -> Reading Raw CSVs...")
    raw_df = spark.read.option("header", "true").csv(raw_source)

    enriched_df = raw_df.withColumn("ingestion_timestamp", current_timestamp()) \
                        .withColumn("source_file", input_file_name())

    print("   -> Writing to Bronze Lake (Delta Format)...")
    enriched_df.write.format("delta").mode("overwrite").save(bronze_path)
    
    print(f" Ingestion Complete. Data is now in Delta Lake at: {bronze_path}")
    spark.stop()

if __name__ == "__main__":
    ingest_to_bronze()