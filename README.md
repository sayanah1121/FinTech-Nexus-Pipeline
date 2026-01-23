# 🏦 FinTech Nexus Pipeline: Enterprise Data Engineering Architecture

**An End-to-End Distributed Data Pipeline for Banking Rewards & Fraud Detection.**

## 🚀 Project Overview
This project implements a scalable **Lakehouse Architecture** to process high-volume banking transactions. It ingests raw transaction streams, applies **Bronze-Silver-Gold** transformations using **Apache Spark**, and orchestrates complex workflows using **Apache Airflow**.

The system features a **Dynamic "Fan-Out/Fan-In" Architecture**, processing multiple vendor streams (Amazon, Flipkart, PayPal) in parallel before aggregating them for Analytics.

## 🏗️ Architecture Stack
* **Orchestration:** Apache Airflow (Dockerized) with Dynamic Task Generation.
* **Processing:** PySpark & Delta Lake (Spark 3.5).
* **Storage:** Local Data Lake (Parquet/Delta) & PostgreSQL (Serving Layer).
* **Infrastructure:** Docker Compose (Custom Images with Java/Python integration).
* **Transformation:** Bronze (Raw) -> Silver (Clean/Enriched) -> Gold (Aggregated).

## ⚡ Key Features
* **Complex DAG Structure:** Implements a "Butterfly Pattern" (Fan-Out processing -> Fan-In aggregation -> Fan-Out Reporting).
* **Custom Docker Infrastructure:** Solved "Dependency Hell" by building custom Airflow images with Java 11 & PySpark support.
* **Security:** Decoupled database credentials using external JSON configuration.
* **Data Quality:** Automated PII masking (SHA-256) and schema validation.

## 🛠️ Setup Instructions
1.  **Clone the Repo:**
    ```bash
    git clone [https://github.com/YOUR_USERNAME/FinTech-Nexus-Pipeline.git](https://github.com/YOUR_USERNAME/FinTech-Nexus-Pipeline.git)
    ```
2.  **Start Infrastructure:**
    ```bash
    docker-compose up -d --build
    ```
3.  **Initialize Airflow:**
    ```bash
    docker-compose run --rm airflow-webserver airflow db migrate
    docker-compose run --rm airflow-webserver airflow users create --username admin --role Admin ...
    ```
4.  **Access UI:** `http://localhost:8080`