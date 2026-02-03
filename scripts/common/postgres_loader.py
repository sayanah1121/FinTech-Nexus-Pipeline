import sys
import os
import psycopg2
from pyspark.sql import SparkSession

def create_dashboard_views():
    print(" Updating Power BI Views in Postgres...")
    
    db_config = {
        "host": "bank_db",
        "port": "5432",
        "database": "finguard_analytics",
        "user": "bank_admin",
        "password": "secure_password"
    }

    sql_commands = [
        # 1. Risk View
        """
        CREATE OR REPLACE VIEW view_fraud_dashboard AS
        SELECT 
            transaction_id, vendor, amount, payment_mode, txn_date,
            risk_severity, risk_reason, is_fraud, investigation_status
        FROM fraud_alerts;
        """,
        
        # 2. Rewards View
        """
        CREATE OR REPLACE VIEW view_rewards_dashboard AS
        SELECT 
            vendor, payment_mode, reward_tier,
            SUM(earned_points) as total_points, 
            COUNT(*) as txn_count
        FROM user_rewards
        GROUP BY vendor, payment_mode, reward_tier;
        """,
        
        # 3. KPI View
        """
        CREATE OR REPLACE VIEW view_executive_kpis AS
        SELECT 
            vendor, report_date, total_volume, txn_count,
            avg_ticket_size, settlement_status
        FROM vendor_kpis;
        """
    ]

    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        for command in sql_commands:
            cur.execute(command)
        conn.commit()
        cur.close()
        conn.close()
        print(" Success: Views updated.")
    except Exception as e:
        print(f"
        Failed to create views (Check if DB is up): {str(e)}")

def load_to_postgres():
    print(" Starting Postgres Load (Serving Layer)...")
    
    spark = SparkSession.builder.appName("FinGuard_Postgres_Loader") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    pg_url = "jdbc:postgresql://bank_db:5432/finguard_analytics"
    pg_properties = {
        "user": "bank_admin",
        "password": "secure_password",
        "driver": "org.postgresql.Driver"
    }

    tables_to_load = {
        "user_rewards": "/opt/airflow/data/lake/gold/banking_rewards",
        "fraud_alerts": "/opt/airflow/data/lake/gold/fraud_alerts",
        "vendor_kpis": "/opt/airflow/data/lake/gold/vendor_stats"
    }

    for table_name, source_path in tables_to_load.items():
        print(f"   -> Loading {table_name}...")
        try:
            df = spark.read.format("delta").load(source_path)
            df.write.jdbc(url=pg_url, table=table_name, mode="overwrite", properties=pg_properties)
        except Exception as e:
            print(f" Failed to load {table_name}: {str(e)}")

    print(" Postgres Loading Complete.")
    spark.stop()
    
    create_dashboard_views()

if __name__ == "__main__":
    load_to_postgres()
