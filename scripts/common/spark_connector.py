import sys
import json
from pyspark.sql import SparkSession

def get_spark_session(app_name):
    """
    Creates a memory-optimized Spark Session for Docker.
    Limits memory to 512MB to allow parallel execution.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def return_xcom(output_path, record_count=0, extra_meta=None):
    """
    Standardizes output for Airflow XComs.
    Prints JSON to STDOUT (captured by Airflow) and logs to STDERR.
    """
    result = {
        "output_path": output_path,
        "record_count": record_count,
        "status": "success"
    }
    if extra_meta:
        result.update(extra_meta)
    
    # Log to Stderr (Visible in Airflow Logs)
    print(f" WORKER SUCCESS: {json.dumps(result)}", file=sys.stderr)
    
    # Print to Stdout (Captured by Airflow XCom)
    print(json.dumps(result))