# Real-Time Stock Market Data Pipeline

## Overview
This project is an end-to-end data engineering solution designed to ingest, process, and visualize high-frequency stock market data in real-time. It demonstrates a **Lambda Architecture** approach, utilizing **Apache Kafka** for streaming ingestion and **Snowflake** for robust warehousing and analytics.

The goal of this project was to simulate a production-grade environment where data reliability, scalability, and latency are critical factors.

## Architecture
![Pipeline Architecture]([INSERT_LINK_TO_YOUR_DIAGRAM_IMAGE_HERE])

The pipeline consists of the following stages:
1.  **Ingestion:** Python scripts fetch real-time data from financial APIs.
2.  **Streaming:** **Apache Kafka** acts as the high-throughput message broker to buffer and decouple data producers from consumers.
3.  **Orchestration:** **Apache Airflow** (running in Docker) manages the workflow dependencies and scheduling.
4.  **Storage (Data Lake):** Raw JSON events are archived in an **MinIO S3** bucket for durability and replayability.
5.  **Warehousing:** Data is loaded into **Snowflake** (Raw Layer).
6.  **Transformation:** **dbt (data build tool)** performs modular transformations to move data from *Raw* to *Cleaned* to *Business Ready* (Medallion Architecture).
7.  **Visualization:** **Power BI** connects to the serving layer for near real-time dashboards.

## Tech Stack
* **Language:** Python 3.12+
* **Containerization:** Docker & Docker Compose
* **Orchestration:** Apache Airflow
* **Streaming:** Apache Kafka & Zookeeper
* **Data Lake:** AWS S3
* **Data Warehouse:** Snowflake
* **Transformation:** dbt Core
* **BI/Visualization:** Power BI

## Key Features
* **Containerized Environment:** The entire infrastructure (Airflow, Kafka, Zookeeper) is defined in `docker-compose.yml` for reproducible deployments.
* **Medallion Architecture:** Implements a structured data flow (Bronze/Silver/Gold) within Snowflake to ensure data quality and governance.
* **Fault Tolerance:** Kafka acts as a buffer to handle backpressure and prevent data loss during API spikes.
* **Infrastructure as Code:** All warehouse tables and pipes are managed via SQL scripts and dbt models.

## Setup & Installation

### Prerequisites
* Docker Desktop installed
* AWS Account (S3 access)
* Snowflake Account
* Python 3.9+

### Running the Pipeline
1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/Alx-Lopez/stock-market-pipeline.git](https://github.com/Alx-Lopez/stock-market-pipeline.git)
    cd stock-market-pipeline
    ```

2.  **Configure Credentials:**
    Create a `.env` file containing your API keys and Snowflake credentials:
    ```env
    AWS_ACCESS_KEY=...
    SNOWFLAKE_USER=...
    API_KEY=...
    ```

3.  **Start Services:**
    Spin up the Docker containers for Airflow and Kafka:
    ```bash
    docker-compose up -d
    ```

4.  **Trigger the DAG:**
    Access the Airflow UI at `http://localhost:8080` and enable the `stock_data_ingestion` DAG.

## Future Improvements
* Implement **Spark Streaming** for complex windowed aggregations before loading to Snowflake.
* Add **Great Expectations** for more granular data quality testing within the Airflow DAG.
* Integrate CI/CD using GitHub Actions for automated dbt testing.
* Look to Deploy in Cloud Solution  