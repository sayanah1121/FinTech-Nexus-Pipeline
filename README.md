# 🛡️ Fintech Nexus Pipeline: Real-Time Fraud & Rewards Engine

![Python](https://img.shields.io/badge/Python-3.9-blue)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-red)
![Spark](https://img.shields.io/badge/PySpark-Processing-orange)
![Docker](https://img.shields.io/badge/Docker-Containerization-blue)
![PowerBI](https://img.shields.io/badge/Power%20BI-Visualization-yellow)

A production-grade Data Engineering pipeline that processes high-volume financial transactions to detect fraud and calculate loyalty rewards in real-time. Designed with a **Fan-Out/Fan-In "Butterfly" Architecture** using Apache Airflow, PySpark, and Delta Lake.

---

## 🏗️ Architecture: The "Butterfly" Pattern

The system follows a distributed processing model to handle multiple vendors simultaneously.

1.  **Ingestion (Head):** Simulates real-time transaction streams from Amazon, Flipkart, PayPal, and Blackhawk.
2.  **Fan-Out (Wings):** Parallel PySpark jobs clean and enrich data for each vendor independently (Silver Layer).
3.  **Fan-In (Body):** Consolidated logic engines process the global stream:
    * **Risk Engine:** 4-Tier Scoring (Critical, High, Moderate, Low).
    * **Rewards Engine:** Calculates loyalty points based on CIBIL score and Payment Mode.
4.  **Serving (Tail):** Data is loaded into PostgreSQL for the **Power BI Command Center**.

---

## 🚀 Key Features

### 1. 🕵️ 4-Tier Fraud Detection Logic
The system doesn't just block transactions; it scores risk severity:
* **CRITICAL (Red):** UPI Transactions > $30,000 (Immediate flag).
* **HIGH (Orange):** Low CIBIL Score (<600) + High Credit Card Spend.
* **MODERATE (Yellow):** Abnormal spending patterns > $10,000.
* **LOW (Green):** Standard activity.

### 2. 💎 Dynamic Rewards Engine
Calculates user "Cashback" and "Loyalty Points" based on complex business rules:
* **Multipliers:** 2x points for Credit Cards, 1.5x for UPI.
* **Tiers:** Bonus points for Platinum Users (CIBIL > 800).

### 3. 📊 Power BI Command Center
A live dashboard connected to the Serving Layer via DirectQuery:
* **Risk Monitor:** Real-time gauge of Critical vs. High risk alerts.
* **Executive View:** Vendor settlement volumes and average ticket size.

---

## 🛠️ Tech Stack

* **Orchestration:** Apache Airflow (2.7.1)
* **Processing:** Apache Spark (PySpark) with Delta Lake
* **Database:** PostgreSQL (Serving Layer)
* **Visualization:** Microsoft Power BI
* **Infrastructure:** Docker & Docker Compose

---

## ⚡ How to Run Locally

### Prerequisites
* Docker Desktop installed (4GB+ RAM allocated).
* Power BI Desktop (for dashboard).

### Steps
1.  **Clone the Repository**
    ```bash
    git clone [https://github.com/sayanah1121/FinTech-Nexus-Pipeline.git](https://github.com/sayanah1121/FinTech-Nexus-Pipeline.git)
    cd FinTech-Nexus-Pipeline
    ```

2.  **Start the Infrastructure**
    ```bash
    docker-compose up -d --build
    ```

3.  **Trigger the Pipeline**
    * Open Airflow at `http://localhost:8080` (User/Pass: `admin`/`admin`).
    * Enable the DAG `finguard_complex_architecture`.
    * Click "Trigger DAG".

4.  **View the Dashboard**
    * Open `FinGuard_Dashboard.pbix` in Power BI.
    * Refresh data to see the latest fraud statistics.

---

## 📂 Project Structure

```text
├── dags/
│   └── finguard_master_dag.py    # Airflow DAG (The Orchestrator)
├── scripts/
│   ├── data_generator.py         # Simulates raw transaction logs
│   ├── silver_cleaning.py        # Standardization Logic
│   ├── gold_fraud_detector.py    # Risk Scoring Logic
│   ├── gold_reward_engine.py     # Loyalty Logic
│   └── postgres_loader.py        # Serving Layer Loader
├── config/
│   └── business_rules.json       # Configurable rules (no code changes needed)
├── docker-compose.yml            # Infrastructure definition
└── requirements.txt              # Python dependencies