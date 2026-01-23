from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'FinGuard_Admin',
    'email_on_failure': False,
    'retries': 0,
}

# The Vendors we want to process in parallel
VENDORS = ['Amazon', 'Flipkart', 'PayPal', 'Blackhawk']

with DAG(
    'finguard_complex_architecture',
    default_args=default_args,
    description='Complex Fan-Out/Fan-In Pipeline',
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['production', 'complex_flow'],
) as dag:

    # 1. Pipeline Start
    start = DummyOperator(task_id='start_pipeline')
    
    # 2. Ingestion (Single Stream)
    generate_data = BashOperator(
        task_id='generate_mock_data',
        bash_command='python /opt/airflow/scripts/data_generator.py'
    )
    
    ingest_bronze = BashOperator(
        task_id='ingest_bronze_lake',
        bash_command='python /opt/airflow/scripts/ingestion_manager.py'
    )

    # 3. Fan-Out: Create Parallel Cleaning & Enrichment Tasks
    # This creates 4 pairs of tasks (Clean -> Enrich) dynamically
    clean_tasks = []
    enrich_tasks = []

    for vendor in VENDORS:
        clean_task = BashOperator(
            task_id=f'clean_{vendor.lower()}',
            bash_command=f'python /opt/airflow/scripts/silver_cleaning.py --vendor {vendor}'
        )
        
        enrich_task = BashOperator(
            task_id=f'enrich_{vendor.lower()}',
            bash_command=f'python /opt/airflow/scripts/silver_enrichment.py --vendor {vendor}'
        )
        
        # Link: Clean -> Enrich
        clean_task >> enrich_task
        
        # Collect them to link to previous/next stages
        clean_tasks.append(clean_task)
        enrich_tasks.append(enrich_task)

    # 4. Fan-In: Analytics (Consolidate all Enriched streams)
    # These tasks read the entire 'enriched' folder
    
    gold_rewards = BashOperator(
        task_id='gold_calc_rewards',
        bash_command='python /opt/airflow/scripts/gold_reward_engine.py'
    )

    gold_fraud = BashOperator(
        task_id='gold_fraud_engine',
        bash_command='python /opt/airflow/scripts/gold_fraud_detector.py'
    )
    
    # 5. Fan-In: Vendor KPIs
    gold_vendor_stats = BashOperator(
        task_id='gold_vendor_kpis',
        bash_command='python /opt/airflow/scripts/gold_vendor_stats.py'
    )

    # 6. Serving: Postgres (Single Node)
    publish_db = BashOperator(
        task_id='publish_to_postgres',
        bash_command='python /opt/airflow/scripts/postgres_loader.py'
    )

    # 7. Fan-Out: Reporting (4 Parallel Tasks)
    report_tasks = []
    for vendor in VENDORS:
        report_task = BashOperator(
            task_id=f'report_{vendor.lower()}',
            bash_command=f'python /opt/airflow/scripts/reverse_etl_reports.py --vendor {vendor}'
        )
        report_tasks.append(report_task)

    end = DummyOperator(task_id='end_pipeline')

    # ==========================================
    # DEPENDENCY WIRING
    # ==========================================
    
    # Start -> Gen -> Ingest
    start >> generate_data >> ingest_bronze
    
    # Ingest -> Fan Out to all Cleaning tasks
    ingest_bronze >> clean_tasks

    # All Enrichment tasks -> Fan In to Gold Engines
    # (This creates the "Bowtie" or "Nerve Center" look)
    for enrich_task in enrich_tasks:
        enrich_task >> gold_rewards
        enrich_task >> gold_fraud
        enrich_task >> gold_vendor_stats

    # Gold -> Postgres (Fan In)
    [gold_rewards, gold_fraud, gold_vendor_stats] >> publish_db

    # Vendor Stats -> Fan Out to Reports
    gold_vendor_stats >> report_tasks

    # Completion
    publish_db >> end
    report_tasks >> end