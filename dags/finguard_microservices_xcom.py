from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

MICROSERVICES = ['amazon', 'flipkart', 'paypal', 'blackhawk']

default_args = {
    'owner': 'FinGuard_Admin',
    'retries': 0, # Debugging mode
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    'finguard_microservices_xcom_v6', # <--- Updated Version ID
    default_args=default_args,
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['production', 'email_report', 'v6'],
    max_active_tasks=1, # Prevents OOM crashes
    concurrency=1 
) as dag:

    start = DummyOperator(task_id='start_pipeline')
    end = DummyOperator(task_id='end_pipeline')

    # --- Shared Engines ---
    gold_fraud = BashOperator(
        task_id='gold_fraud', 
        bash_command='python /opt/airflow/scripts/common/gold_fraud_detector.py'
    )
    gold_stats = BashOperator(
        task_id='gold_stats', 
        bash_command='python /opt/airflow/scripts/common/gold_vendor_stats.py'
    )
    gold_rewards = BashOperator(
        task_id='gold_rewards', 
        bash_command='python /opt/airflow/scripts/common/gold_reward_engine.py'
    )
    publish_db = BashOperator(
        task_id='publish_db', 
        bash_command='python /opt/airflow/scripts/common/postgres_loader.py'
    )

    # 📧 NEW EMAIL TASK
    email_report = BashOperator(
        task_id='send_email_report',
        bash_command='python /opt/airflow/scripts/common/email_reporter.py'
    )

    # --- Dynamic Microservices Loop ---
    enrich_tasks = []

    for vendor in MICROSERVICES:
        # 1. GENERATE
        gen = BashOperator(
            task_id=f'gen_{vendor}',
            bash_command=f'python /opt/airflow/scripts/{vendor}/generate.py',
            do_xcom_push=True 
        )

        # 2. CLEAN (Robust JSON parsing)
        clean_cmd = f"""
            raw_xcom='{{{{ ti.xcom_pull(task_ids="gen_{vendor}") }}}}'
            path=$(python -c "import json, sys; 
try:
    data = json.loads('''$raw_xcom''')
    print(data['output_path'])
except:
    sys.exit(1)")

            if [ -z "$path" ]; then echo "❌ FATAL: XCom Failed"; exit 1; fi
            python /opt/airflow/scripts/{vendor}/clean.py --input_path "$path"
        """
        
        clean = BashOperator(
            task_id=f'clean_{vendor}',
            bash_command=clean_cmd,
            do_xcom_push=True
        )

        # 3. ENRICH (Robust JSON parsing)
        enrich_cmd = f"""
            raw_xcom='{{{{ ti.xcom_pull(task_ids="clean_{vendor}") }}}}'
            path=$(python -c "import json, sys; 
try:
    data = json.loads('''$raw_xcom''')
    print(data['output_path'])
except:
    sys.exit(1)")

            if [ -z "$path" ]; then echo "❌ FATAL: XCom Failed"; exit 1; fi
            python /opt/airflow/scripts/{vendor}/enrich.py --input_path "$path"
        """
        
        enrich = BashOperator(
            task_id=f'enrich_{vendor}',
            bash_command=enrich_cmd,
            do_xcom_push=True
        )

        start >> gen >> clean >> enrich
        enrich_tasks.append(enrich)

    # --- Fan In Wiring ---
    
    # 1. All microservices feed the Gold Layer
    for enrich in enrich_tasks:
        enrich >> gold_fraud
        enrich >> gold_stats
        enrich >> gold_rewards

    # 2. Gold Layer feeds Database & Email Report
    [gold_fraud, gold_stats, gold_rewards] >> publish_db
    [gold_fraud, gold_stats, gold_rewards] >> email_report
    
    # 3. End
    [publish_db, email_report] >> end