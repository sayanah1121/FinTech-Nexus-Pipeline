import sys, os, random, pandas as pd, numpy as np
from datetime import datetime, timedelta
sys.path.append("/opt/airflow/scripts")
from common.spark_connector import return_xcom

def run():
    VENDOR = "blackhawk"
    print(f" [{VENDOR}] Generating...", file=sys.stderr)
    
    total = 5000
    denominations = [25.0, 50.0, 100.0, 200.0, 500.0]
    data = {
        "txn_id": [f"BH-{i:09d}" for i in range(total)],
        "user_id": [f"USR-{random.randint(1, 50000)}" for _ in range(total)],
        "amount": [random.choice(denominations) for _ in range(total)],
        "payment_mode": ["Gift Card Redemption" for _ in range(total)],
        "vendor": "Blackhawk",
        "txn_date": [(datetime.now() - timedelta(days=random.randint(0, 7))).strftime('%Y-%m-%d %H:%M:%S') for _ in range(total)]
    }

    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"/opt/airflow/data/raw/{VENDOR}/{batch_id}"
    if not os.path.exists(path): os.makedirs(path)
    
    pd.DataFrame(data).to_csv(f"{path}/data.csv", index=False)
    return_xcom(path, total)

if __name__ == "__main__": run()