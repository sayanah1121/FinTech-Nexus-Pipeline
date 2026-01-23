import pandas as pd
import numpy as np
import random
import os
import time
from datetime import datetime, timedelta

def generate_bulk_data(total_records=1000000):
    print(f" Starting generation of {total_records} records...")
    
    vendors = ['Amazon', 'Flipkart', 'PayPal', 'Blackhawk']
    payment_modes = ['Credit Card', 'Debit Card', 'UPI', 'NetBanking']
    
    # 1. Generate Transaction Data
    data = {
        "txn_id": [f"TXN-{i:07d}" for i in range(total_records)],
        "user_id": [f"USR-{random.randint(1, 50000)}" for _ in range(total_records)],
        "amount": np.round(np.random.uniform(10.5, 50000.0, total_records), 2),
        "payment_mode": [random.choice(payment_modes) for _ in range(total_records)],
        "vendor": [random.choice(vendors) for _ in range(total_records)],
        "txn_date": [(datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d %H:%M:%S') for _ in range(total_records)]
    }

    df = pd.DataFrame(data)
    
    # 2. Path for Docker/Airflow
    base_path = "/opt/airflow/data/raw"
    
    for vendor in vendors:
        vendor_path = f"{base_path}/{vendor.lower()}"
        if not os.path.exists(vendor_path):
            os.makedirs(vendor_path)
            
        output_file = f"{vendor_path}/daily_dump.csv"
        df[df['vendor'] == vendor].to_csv(output_file, index=False)
        print(f"   -> Generated {len(df[df['vendor'] == vendor])} rows for {vendor}")

    # 3. Generate CIBIL Master Reference Data
    cibil_data = {
        "user_id": [f"USR-{i}" for i in range(1, 50001)],
        "cibil_score": [random.randint(300, 900) for _ in range(50000)],
        "account_tier": [random.choice(['Regular', 'Silver', 'Gold', 'Platinum']) for _ in range(50000)]
    }
    pd.DataFrame(cibil_data).to_csv("/opt/airflow/data/reference/user_cibil_master.csv", index=False)
    
    print(" Phase 1 Data Generation Complete.")

if __name__ == "__main__":
    generate_bulk_data()