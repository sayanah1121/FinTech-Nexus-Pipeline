import sys
import os
from pyspark.sql import SparkSession

def load_to_postgres():
    print(" Starting Postgres Load (Serving Layer)...")
    
    # Initialize Spark with Postgres JDBC Driver
    spark = SparkSession.builder.appName("FinGuard_Postgres_Loader") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Configuration for "bank_db" container
    # Note: Inside Docker network, we use service name 'bank_db'.
    # If testing locally on Windows, change 'bank_db' to 'localhost' and port to '5435'
    pg_url = "jdbc:postgresql://bank_db:5432/finguard_analytics"
    pg_properties = {
        "user": "bank_admin",
        "password": "secure_password",
        "driver": "org.postgresql.Driver"
    }

    # Define Mappings: Source Path -> Destination Table
    tables_to_load = {
        "user_rewards": "/opt/airflow/data/lake/gold/banking_rewards",
        "fraud_alerts": "/opt/airflow/data/lake/gold/fraud_alerts",
        "vendor_kpis": "/opt/airflow/data/lake/gold/vendor_stats"
    }

    for table_name, source_path in tables_to_load.items():
        print(f"   -> Loading {table_name} from {source_path}...")
        
        try:
            df = spark.read.format("delta").load(source_path)
            
            # Write to Postgres (Overwrite mode for idempotency)
            df.write.jdbc(url=pg_url, table=table_name, mode="overwrite", properties=pg_properties)
            print(f"       Success: {table_name} loaded.")
            
        except Exception as e:
            print(f"       Failed to load {table_name}: {str(e)}")

    print(" Postgres Loading Complete.")
    spark.stop()

if __name__ == "__main__":
    load_to_postgres()