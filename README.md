# 📞 Telecommunications Analytics Pipeline

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-F68A1E?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

## 📖 Overview
This project provides a robust, production-style batch analytics pipeline designed to process **2M+ Call Detail Records (CDRs)** at scale. Leveraging the Hadoop/Spark ecosystem and orchestrated by Apache Airflow, this pipeline ensures high-throughput data processing, effective data skew management, and automated job reporting.

## 🏗️ Project Structure
```text
project-root/
├── .env.example             # Environment configuration templates
├── docker-compose.yml       # Infrastructure orchestration
├── Dockerfile               # Spark application environment
├── README.md                # Project documentation
├── run_pipeline.sh          # CLI tool to trigger DAGs
├── data/
│   └── generate_records.sh  # Dataset simulation script
├── jobs/                    # PySpark job implementations
│   ├── top_callers.py
│   ├── tower_heatmap.py
│   ├── anomalous_calls.py
│   └── revenue_recon.py
├── dags/                    # Airflow workflow definitions
│   ├── top_callers_dag.py
│   ├── tower_heatmap_dag.py
│   ├── anomalous_calls_dag.py
│   └── revenue_recon_dag.py
└── output/                  # Job results and manifests
```

## 🧠 System Design Decisions

- **Orchestration Logic**: We utilize Apache Airflow for DAG orchestration to provide fault-tolerant, retryable job execution. Using the `run_pipeline.sh` script as a CLI wrapper provides a clean interface that abstracts the Airflow DAG triggering logic.
- **Data Skew Strategy ("Whale Caller")**: A "whale caller" (`caller_whale`) accounts for 10% of the dataset. To manage this skew and avoid severe hotspots on the Spark workers, the `anomalous_calls` job utilizes a **custom RDD Partitioner** (`custom_partitioner`). This partitioner isolates the skewed `"caller_whale"` key entirely into Partition 0, while distributing the remaining callers across Partitions 1–9 using a hash modulo strategy. This prevents the whale caller's partition from colliding with other callers, balancing worker load and ensuring that the statistical calculations (mean and standard deviation) are highly performant.
- **Scalability**: By leveraging the Hadoop/Spark ecosystem via `docker-compose`, the pipeline is horizontally scalable. The design is optimized for large-scale enterprise workloads, such as revenue assurance and network analysis.

## 🚀 Getting Started

### Prerequisites
- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) installed on your machine.

### Setup Instructions
1. **Clone this repository** to your local machine.
   ```bash
   git clone https://github.com/Shanmuka-p/Hadoop-Based-Batch-Analytics.git
   cd Hadoop-Based-Batch-Analytics
   ```
2. **Ensure scripts are executable**:
   ```bash
   chmod +x run_pipeline.sh
   chmod +x data/generate_records.sh
   ```
3. **Start the infrastructure**:
   ```bash
   docker-compose up --build
   ```
4. **Wait for all services** (Namenode, Spark, Airflow) to report a "healthy" status.

### Running the Pipeline
Use the provided `run_pipeline.sh` script to trigger specific analytics jobs:

```bash
./run_pipeline.sh top_callers
./run_pipeline.sh tower_heatmap
./run_pipeline.sh anomalous_calls
./run_pipeline.sh revenue_recon
```

## 📊 Execution Monitoring
Each job generates an `_MANIFEST.json` file in its respective output directory (e.g., `/output/top_callers_by_spend/{run_id}/_MANIFEST.json`). This file summarizes the execution metadata, including:
- **Job Name**
- **Run ID**
- **Input/Output paths**
- **Record counts**
- **Execution status**
