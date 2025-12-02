# ✈️ Airline Data Engineering Pipeline

A complete ELT pipeline built with **Apache Airflow** and **Snowflake**.
It processes airline passenger data using a layered architecture (Raw -> DWH -> Data Mart).

## 🏗 Architecture

**Stack:** Airflow (Docker), Snowflake, Python, SQL.

1.  **Ingestion:** Python script uploads CSV to Snowflake Stage using `PUT`.
2.  **Raw Layer:** Airflow triggers `COPY INTO` to load data into the `RAW` table.
3.  **DWH Layer:** Stored Procedure (SQL/JS) processes data using `INSERT ... NOT EXISTS`, handling deduplication and logging.
4.  **Data Mart:** Aggregated statistics by country are calculated and stored in the `DM` layer.
5.  **CDC:** Used Snowflake Streams to process only new data.

## 🚀 Key Features

* **Infrastructure as Code:** Airflow runs in Docker containers.
* **Idempotency:** Pipeline can be re-run multiple times without creating duplicates.
* **Audit Logging:** Custom logging table (`UTILS.AUDIT_LOG`) tracks every step.
* **Time Travel:** Configured Snowflake Time Travel for data recovery.

## 🛠 How to Run

1.  Clone the repo:
    ```bash
    git clone [https://github.com/to4er/snowflake-airflow-task.git](https://github.com/to4er/snowflake-airflow-task.git)
    ```
2.  Start Airflow:
    ```bash
    docker compose up -d
    ```
3.  Configure Snowflake connection in Airflow UI (`snowflake_default`).
4.  Run the scripts in Snowflake to create tables.
5.  Trigger the `main_pipeline` DAG.