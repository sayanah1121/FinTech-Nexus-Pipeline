import smtplib
import os
import sys
import pandas as pd
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from pyspark.sql import SparkSession

# --- CONFIGURATION (Change these or use Env Vars) ---
SENDER_EMAIL = "sayansarkar0612@gmail.com"  # <--- REPLACE THIS
SENDER_APP_PASSWORD = "yxze jsuf mxap cwkk" # <--- REPLACE WITH YOUR APP PASSWORD
RECEIVER_EMAIL = "sayansarkar1121@gmail.com"

def get_spark_session():
    return SparkSession.builder \
        .appName("FinGuard_Email_Reporter") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()

def generate_excel_report():
    print(" Generating Gold Layer Excel Report...")
    spark = get_spark_session()
    
    # Define Gold Paths
    base_path = "/opt/airflow/data/lake/gold"
    report_date = datetime.now().strftime("%Y-%m-%d")
    file_path = f"/tmp/FinGuard_Report_{report_date}.xlsx"

    # Read Data (Spark -> Pandas)
    # Note: We limit to 1000 rows for email attachment size safety
    try:
        df_fraud = spark.read.format("delta").load(f"{base_path}/fraud_alerts").limit(1000).toPandas()
        df_rewards = spark.read.format("delta").load(f"{base_path}/banking_rewards").limit(1000).toPandas()
        df_stats = spark.read.format("delta").load(f"{base_path}/vendor_stats").toPandas()
        
        # Write to Excel with Multiple Sheets
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df_fraud.to_excel(writer, sheet_name='Fraud Alerts', index=False)
            df_rewards.to_excel(writer, sheet_name='Loyalty Rewards', index=False)
            df_stats.to_excel(writer, sheet_name='Vendor KPIs', index=False)
            
        print(f" Excel generated at: {file_path}")
        return file_path
    except Exception as e:
        print(f" Error reading Gold Layer: {e}")
        sys.exit(1)

def send_email(attachment_path):
    print(f" Sending email to {RECEIVER_EMAIL}...")
    
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECEIVER_EMAIL
    msg['Subject'] = f"FinGuard Daily Report - {datetime.now().strftime('%Y-%m-%d')}"

    body = """
    Hello Team,

    Please find attached the daily Gold Layer Summary from the FinGuard Data Pipeline.
    
    Contains:
    1. Fraud Alerts (Critical/High Risk)
    2. Loyalty Rewards Calculated
    3. Vendor Settlement KPIs

    Regards,
    FinGuard Bot 
    """
    msg.attach(MIMEText(body, 'plain'))

    # Attach File
    with open(attachment_path, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    
    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {os.path.basename(attachment_path)}",
    )
    msg.attach(part)

    # SMTP Send
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_APP_PASSWORD)
        text = msg.as_string()
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, text)
        server.quit()
        print(" Email sent successfully!")
    except Exception as e:
        print(f" SMTP Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    report_path = generate_excel_report()
    send_email(report_path)