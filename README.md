# 🛡️ FinGuard: Enterprise Data Lake & Fraud Detection System

![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.1-blue?style=for-the-badge&logo=apacheairflow)
![Spark](https://img.shields.io/badge/PySpark-3.4.1-orange?style=for-the-badge&logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0.0-cyan?style=for-the-badge&logo=deltalake)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue?style=for-the-badge&logo=docker)
![Postgres](https://img.shields.io/badge/PostgreSQL-Serving-336791?style=for-the-badge&logo=postgresql)

**FinGuard** is a production-grade Data Engineering platform designed to simulate a real-world Fintech ecosystem. It ingests transaction data from multiple "Microservice" vendors (Amazon, Flipkart, PayPal, Blackhawk), processes them in parallel using **Apache Spark**, detects fraud patterns, calculates loyalty rewards, and generates automated C-Suite reports.

The project demonstrates an **Event-Driven "Butterfly" Architecture** (Fan-Out/Fan-In) orchestrated by **Apache Airflow**.

---

## 🏗️ System Architecture

The pipeline follows a strict **Bronze ➔ Silver ➔ Gold** Medallion Architecture:

1.  **Microservices Layer (Fan-Out):**
    * Independent generation, cleaning, and enrichment pipelines for 4 distinct vendors.
    * **Dynamic XComs:** Airflow tasks pass data paths dynamically to downstream tasks, ensuring data integrity.
    * **Resource Isolation:** Each Spark job runs in a dedicated ephemeral process to prevent Memory Leaks (OOM).

2.  **Consolidated Logic Layer (Fan-In):**
    * **Fraud Engine:** Merges all 4 streams to detect cross-vendor anomalies (e.g., "Velocity Checks" or "High-Risk UPI").
    * **Rewards Engine:** Calculates loyalty points based on CIBIL scores and payment modes.
    * **Vendor Stats:** Aggregates settlement volumes for KPIs.

3.  **Serving & Reporting Layer:**
    * **PostgreSQL:** Final Gold tables are loaded into a relational DB for dashboarding (PowerBI/Tableau).
    * **Email Reporter:** An automated bot generates an Excel Report with 3 sheets (Fraud, Rewards, Stats) and emails it to stakeholders using SMTP.


📂 Project Structure
The project uses a Domain-Driven Design where each vendor has its own isolated logic folder.

Plaintext
FinGuard-Pipeline/
├── dags/
│   └── finguard_microservices_xcom.py   # Main Orchestrator (DAG v6)
├── scripts/
│   ├── common/                          # Shared Utilities
│   │   ├── spark_connector.py           # Memory-Safe Spark Session Factory
│   │   ├── email_reporter.py            # Automated SMTP Excel Reporter
│   │   ├── gold_fraud_detector.py       # Central Risk Logic
│   │   ├── gold_reward_engine.py        # Central Loyalty Logic
│   │   ├── gold_vendor_stats.py         # Central KPI Aggregator
│   │   └── postgres_loader.py           # Database Loader
│   ├── amazon/                          # Amazon Microservice
│   │   ├── generate.py
│   │   ├── clean.py
│   │   └── enrich.py
│   ├── flipkart/                        # Flipkart Microservice (etc...)
│   ├── paypal/
│   └── blackhawk/
├── config/
│   └── business_rules.json              # Configurable Thresholds
├── data/                                # Local Data Lake (Mounted Volume)
├── docker-compose.yml                   # Infrastructure (Airflow, Postgres, Kafka)
└── Dockerfile                           # Custom Image with PySpark & Java 11
🚀 Key Features
1. Robust "XCom" Data Passing
Instead of hardcoding file paths, the Generator tasks return the specific path of the data batch they created. Cleaner and Enricher tasks dynamically read only that batch.

Benefit: Prevents processing stale data and allows for backfilling historical runs.

2. OOM-Safe Spark Execution
Using BashOperator combined with a custom spark_connector.py, each task is limited to 512MB RAM.

Benefit: Allows running heavy Spark jobs on a standard laptop (Docker) without crashing the container due to "Out of Memory" errors.

3. Automated Executive Reporting
A Python-based email_reporter pulls Gold Layer data, formats it into a multi-sheet Excel file using openpyxl, and emails it to the admin via Gmail SMTP.

4. 4-Tier Risk Scoring
The Fraud Engine assigns severity levels:

🔴 CRITICAL: UPI transactions > $30,000.

🟠 HIGH: Credit Card usage with low CIBIL Score (<600).

🟡 MODERATE: High-value anomalies.

🟢 LOW: Standard transactions.

⚡ Setup & Installation
Prerequisites
Docker Desktop (Allocated: 4GB+ RAM)

Git

Step 1: Clone the Repository
Bash
git clone [https://github.com/sayanah1121/finguard-pipeline.git](https://github.com/sayanah1121/finguard-pipeline.git)
cd finguard-pipeline
Step 2: Configure Credentials
Open scripts/common/email_reporter.py.

Update SENDER_EMAIL and SENDER_APP_PASSWORD with your Google App Password.

Note: Do not use your regular Gmail password. Generate an App Password via Google Account > Security.

Step 3: Build Infrastructure
Bash
docker-compose up -d --build
Wait 30-60 seconds for Postgres and Airflow to initialize.

Step 4: Initialize Airflow (First Run Only)
If you just built the containers, you need to create the database schema and admin user.

PowerShell (Windows):

PowerShell
docker-compose run --rm airflow-webserver airflow db migrate
docker-compose run --rm airflow-webserver airflow users create --username admin --firstname FinGuard --lastname Admin --role Admin --email admin@finguard.com --password admin
Bash (Mac/Linux):

Bash
docker-compose run --rm airflow-webserver airflow db migrate
docker-compose run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname FinGuard \
    --lastname Admin \
    --role Admin \
    --email admin@finguard.com \
    --password admin
Step 5: Run the Pipeline
Open http://localhost:8080.

Login with admin / admin.

Find DAG: finguard_microservices_xcom_v6.

Toggle ON and click the Play button.

📊 Dashboard Views (PostgreSQL)
Once the pipeline finishes, you can connect PowerBI or Tableau to the finguard_bank_db (Port 5435) to visualize:

View 1: view_fraud_dashboard (Real-time alert monitor)

View 2: view_rewards_dashboard (Loyalty tier distribution)

View 3: view_executive_kpis (Settlement volumes by vendor)

🛠️ Troubleshooting
Issue: "Task exited with return code 1" / "XCom Failed"

Fix: Check the logs of the previous task (e.g., if clean fails, check generate). Ensure your generate script is printing valid JSON to stdout.

Issue: "Blank Logs" / "Up for Retry"

Fix: Your Docker is out of RAM. The DAG is configured with max_active_tasks=1 to prevent this, but you may need to increase Docker Desktop memory limit to 6GB.

👨‍💻 Author
Sayan Sarkar

Role: Data Engineer | Robotics Software Engineer

Stack: Python, SQL, Spark, Airflow, Docker, AWS