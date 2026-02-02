import sys, os, random, pandas as pd, numpy as np
from datetime import datetime, timedelta
sys.path.append("/opt/airflow/scripts")
from common.spark_connector import return_xcom

def run():
    VENDOR = "amazon"
    print(f" [{VENDOR}] Generating...", file=sys.stderr)
    
    total = 5000
    data = {
        "txn_id": [f"AMZ-{i:07d}" for i in range(total)],
        "user_id": [f"USR-{random.randint(1, 50000)}" for _ in range(total)],
        "amount": np.round(np.random.uniform(10.0, 5000.0, total), 2),
        "payment_mode": [random.choice(['Credit Card', 'UPI', 'NetBanking']) for _ in range(total)],
        "vendor": VENDOR,
        "txn_date": [(datetime.now() - timedelta(days=random.randint(0, 3))).strftime('%Y-%m-%d %H:%M:%S') for _ in range(total)]
    }

    # Dynamic Batch Path
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"/opt/airflow/data/raw/{VENDOR}/{batch_id}"
    if not os.path.exists(path): os.makedirs(path)
    
    pd.DataFrame(data).to_csv(f"{path}/data.csv", index=False)
    return_xcom(path, total)

if __name__ == "__main__": run()