# 📞 Telecommunications Analytics Pipeline

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-F68A1E?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

## 📖 Overview

This project provides a robust, production-style batch analytics pipeline designed to simulate, load, and process **2M+ Call Detail Records (CDRs)** at scale. Leveraging a containerized Hadoop/Spark ecosystem and orchestrated by Apache Airflow, this pipeline ensures high-throughput data processing, effective data skew management, and automated job reporting with full execution metadata manifests.

---

## 🏗️ Project Structure

Below is an overview of the key files and folders in the workspace:

```text
Hadoop-Based-Batch-Analytics/
├── .env.example                     # Template for configuring environment variables
├── docker-compose.yml               # Container orchestration (Hadoop, Spark, Airflow, Generator)
├── Dockerfile                       # PySpark application environment definition
├── Dockerfile.airflow               # Airflow runtime with JDK-17, Spark Providers, and PySpark
├── README.md                        # Documentation (this file)
├── run_pipeline.sh                  # CLI wrapper tool to trigger Airflow DAGs
├── data/                            # Volume-mounted dataset simulator
│   ├── generate_records.py          # Python record generator script (2M+ records)
│   └── generate_records.sh          # Shell wrapper to execute generation
├── jobs/                            # PySpark analytical job implementations
│   ├── anomalous_calls.py           # Statistical anomaly detection using custom RDD partitioning
│   ├── revenue_recon.py             # Revenue reconciliation calculations
│   ├── top_callers.py               # Spender analytics ranking the top 100 callers
│   └── tower_heatmap.py             # Cell tower utilization heatmap per hour
├── dags/                            # Apache Airflow workflow definitions
│   ├── anomalous_calls_dag.py       # DAG to schedule/run anomalous call analysis
│   ├── revenue_recon_dag.py         # DAG to schedule/run revenue reconciliation
│   ├── top_callers_dag.py           # DAG to schedule/run caller ranking
│   └── tower_heatmap_dag.py         # DAG to schedule/run tower utilization
└── output/                          # Shared volume for job output and run manifests
```

- **Configuration**: [.env.example](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/.env.example) and [docker-compose.yml](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/docker-compose.yml)
- **Environments**: [Dockerfile](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/Dockerfile) and [Dockerfile.airflow](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/Dockerfile.airflow)
- **Entry Points**: [run_pipeline.sh](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/run_pipeline.sh) and [data/generate_records.sh](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/data/generate_records.sh)
- **Data Engine**: [data/generate_records.py](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/data/generate_records.py)
- **PySpark Jobs**: [jobs/top_callers.py](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/jobs/top_callers.py), [jobs/tower_heatmap.py](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/jobs/tower_heatmap.py), [jobs/anomalous_calls.py](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/jobs/anomalous_calls.py), and [jobs/revenue_recon.py](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/jobs/revenue_recon.py)
- **Airflow DAGs**: [dags/top_callers_dag.py](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/dags/top_callers_dag.py), [dags/tower_heatmap_dag.py](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/dags/tower_heatmap_dag.py), [dags/anomalous_calls_dag.py](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/dags/anomalous_calls_dag.py), and [dags/revenue_recon_dag.py](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/dags/revenue_recon_dag.py)

---

## ⚙️ Service Infrastructure & Ports

The application spans multiple containerized services orchestrated via `docker-compose.yml`. Use the table below to monitor infrastructure endpoints:

| Service Name | Container Name | Port (Host:Guest) | Web UI / Health Check Endpoint | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Namenode** | `namenode` | `9870:9870` | [http://localhost:9870](http://localhost:9870) | Hadoop HDFS Namenode Web UI |
| **Spark Master** | `spark-master` | `8080:8080`, `7077:7077` | [http://localhost:8080](http://localhost:8080) | Spark Cluster Manager Web UI & master socket |
| **Spark Worker** | `spark-worker` | N/A | N/A | Worker node registered to Spark Master |
| **Data Generator** | `data-generator` | N/A | N/A | Generates raw dataset on startup |
| **Airflow Server** | `airflow` | `8081:8080` | [http://localhost:8081](http://localhost:8081) | Airflow Standalone Web UI & CLI node |

---

## 🧠 Core System Design Decisions

- **Orchestration Architecture**: We utilize Apache Airflow for robust, retryable DAG orchestration. Airflow communicates with the Spark cluster using the `SparkSubmitOperator` under the connection alias `spark_default`. The `run_pipeline.sh` script acts as a CLI wrapper that abstracts triggering logic using `docker exec`.
- **Data Skew Mitigation ("Whale Caller")**: A deterministic 10% of the generated CDRs belong to a simulated heavy-user `caller_whale`. Processing this key conventionally causes data skew, leading to imbalanced worker loads and execution hotspots. To handle this, the `anomalous_calls` job leverages a **custom RDD Partitioner** (`custom_partitioner`):
  - Isolates `"caller_whale"` exclusively into Partition 0.
  - Dynamically routes other callers across Partitions 1–9 using a hash-modulo strategy: `(hash(key) % 9) + 1`.
  - Ensures statistical mean/stddev computations run with optimized parallelization without workers bottlenecking on the skewed partition.

---

## 🚀 Step-by-Step running Guide

Follow these sequential steps to set up, launch, run, and monitor the batch analytics pipeline:

### Step 1: Prerequisites
Ensure you have the following software installed:
- [Docker Engine (v20.10+)](https://docs.docker.com/engine/install/)
- [Docker Compose (v2.0+)](https://docs.docker.com/compose/install/)

### Step 2: Clone and Navigate
Clone this repository and open the project directory:
```bash
git clone https://github.com/Shanmuka-p/Hadoop-Based-Batch-Analytics.git
cd Hadoop-Based-Batch-Analytics
```

### Step 3: Setup Configuration Files
Copy the `.env.example` file to create a local `.env` configuration file:
```bash
cp .env.example .env
```
*(This file defines connection coordinates such as Spark Masters and Hadoop Namenodes).*

### Step 4: Grant Executable Permissions
If running in a Unix-like environment (Linux/macOS/Git Bash/WSL), make sure the utility shell scripts have executable permissions:
```bash
chmod +x run_pipeline.sh
chmod +x data/generate_records.sh
```

### Step 5: Start the Container Infrastructure
Build and launch the Docker services in the background using:
```bash
docker-compose up --build -d
```
*This command pulls images, builds custom Spark and Airflow containers, and launches services in detached mode.*

### Step 6: Verify Infrastructure Startup
Monitor startup health checks using `docker ps`. You must wait until all containers are healthy.
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```
Verify that:
- `namenode` reports `(healthy)`
- `spark-master` reports `(healthy)`
- `data-generator` reports `(healthy)` (which implies data generation is complete!)
- `airflow` reports `(healthy)`

### Step 7: Retrieve Airflow Web UI Admin Password
Airflow runs in `standalone` mode, which automatically generates a random admin password during initialization. To retrieve this password, run:
```bash
docker exec -it airflow cat /opt/airflow/standalone_admin_password.txt
```
Copy the printed password. You can now log in to the Airflow UI at [http://localhost:8081](http://localhost:8081) with:
- **Username**: `admin`
- **Password**: *[Paste the copied password]*

---

## 📊 Phase 1: Data Generation & Verification

On startup, the `data-generator` container automatically executes [data/generate_records.sh](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/data/generate_records.sh) to simulate the Call Detail Records. 

### Verify Generated Data
The records are output to the shared volume under `./data/cdr_data.csv`. You can verify the file is successfully created and inspect its volume by executing:
```bash
# Verify file presence and line counts (should be 2,000,101 lines including header)
docker exec -it airflow wc -l /data/cdr_data.csv

# Inspect the top 5 records of the simulated dataset
docker exec -it airflow head -n 5 /data/cdr_data.csv
```

### Dataset Schema Reference
The generated CSV follows this structure:
```csv
caller_id,receiver_id,duration_sec,tower_id,timestamp,call_type,charge_amount
caller_whale,receiver_734,228,tower_24,2026-06-03 01:14:15,VOICE,0.12
caller_382,receiver_458,112,tower_12,2026-06-05 18:32:01,SMS,0.08
```

---

## ⚡ Phase 2: Running Analytics Pipeline Jobs

There are two ways to trigger and execute the PySpark analytics jobs.

### Method A: Using the CLI Wrapper Script (Recommended)
We provide a helper script [run_pipeline.sh](file:///d:/Partnr/Main/week23/Hadoop-Based-Batch-Analytics/run_pipeline.sh) that maps jobs to Airflow DAGs and triggers executions inside the Airflow environment:

1. **Top Callers by Spend**:
   ```bash
   ./run_pipeline.sh top_callers
   ```
2. **Cell Tower Heatmap**:
   ```bash
   ./run_pipeline.sh tower_heatmap
   ```
3. **Anomalous Call Detection**:
   ```bash
   ./run_pipeline.sh anomalous_calls
   ```
4. **Revenue Reconciliation**:
   ```bash
   ./run_pipeline.sh revenue_recon
   ```

### Method B: Via Airflow Web Interface
1. Navigate to the Airflow UI at [http://localhost:8081](http://localhost:8081).
2. Unpause/Enable the DAG you want to run by toggling the switch next to its name.
3. Click the **Trigger DAG** (Play button) on the right side.
4. Optionally, provide a custom configuration JSON, or click **Trigger** to run with default configs.

---

## 🔍 Phase 3: Monitoring & Validating Results

### 1. Monitor Job Progress
- **Airflow Web UI** ([http://localhost:8081](http://localhost:8081)): Track the execution graphs, task instances, logs, and retries.
- **Spark Web UI** ([http://localhost:8080](http://localhost:8080)): Track active stages, execution plans, worker loads, and resource allocations.

### 2. Inspect Job Outputs
Once a job completes successfully, the results are saved in the host's `./output/` directory (mapped from the container's `/output/`). Outputs are organized by run ID:

- **Top Callers**: `./output/top_callers_by_spend/{run_id}/`
- **Tower Heatmap**: `./output/tower_utilization_heatmap/{run_id}/`
- **Anomalous Calls**: `./output/anomalous_call_detection/{run_id}/`
- **Revenue Recon**: `./output/revenue_reconciliation/{run_id}/`

### 3. Review Job Execution Manifests
Each execution folder produces an `_MANIFEST.json` containing execution metadata. Example layout:

```json
{
  "job_name": "top_callers_by_spend",
  "run_id": "20260613_153022",
  "execution_timestamp_utc": "2026-06-13T15:31:05.123456",
  "input_path": "/data/cdr_data.csv",
  "output_path": "/output/top_callers_by_spend/20260613_153022",
  "input_record_count": 2000100,
  "output_record_count": 100,
  "status": "SUCCESS"
}
```

You can view the manifest directly from your command line:
```bash
# Example command (replace <run_id> with your actual execution folder name)
cat ./output/top_callers_by_spend/<run_id>/_MANIFEST.json
```

---

## 🧹 Tearing Down the Infrastructure

To stop all services and free up host resources, execute:
```bash
docker-compose down
```
*Note: This command stops and removes the containers but preserves the generated data in `./data/` and execution outputs in `./output/` volumes.*
