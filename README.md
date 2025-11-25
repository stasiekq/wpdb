# Mini Data Platform - WPDB Project

A mini data platform implementation using Docker containers that simulates a business process, ingests data into PostgreSQL, captures changes with Debezium, streams data through Kafka, and processes it in Spark.

## Project Structure

```
wpdb/
├── src/              # Source code
│   ├── task1/        # Task 1: CSV to PostgreSQL loader
│   ├── task3/        # Task 3: Kafka AVRO consumer
│   ├── task4/        # Task 4: Spark streaming job
│   └── utils/        # Utility scripts
├── config/           # Configuration files
│   └── debezium/     # Debezium connector configurations
├── scripts/          # Execution and utility scripts
├── docs/             # Documentation
├── data/             # CSV data files
└── logs/             # Application logs
```

## Quick Start

1. **Set up environment:**
   ```bash
   cp .env.example .env  # Edit .env with your settings
   ```

2. **Start the pipeline:**
   ```bash
   ./scripts/run_pipeline.sh
   # or
   python3 scripts/run_pipeline.py
   ```

3. **Run Spark streaming job (Task 4):**
   ```bash
   ./scripts/run_spark_job.sh
   ```

4. **Run Spark Delta Lake job (Task 5):**
   ```bash
   ./scripts/run_spark_delta_job.sh
   ```

5. **Read Delta Lake data:**
   ```bash
   docker exec spark /opt/spark/bin/spark-submit \
     --master spark://spark:7077 \
     --packages io.delta:delta-spark_2.12:3.0.0 \
     /opt/spark/apps/read_delta.py
   ```

## Components

### Task 1: Business Process Simulation
- **Script:** `src/task1/csv_loader.py`
- Reads CSV files and loads them into PostgreSQL
- Creates tables dynamically based on CSV structure

### Task 2: Debezium CDC
- **Config:** `config/debezium/postgres_connector.json`
- Captures PostgreSQL changes and sends to Kafka in AVRO format

### Task 3: Kafka & Schema Registry
- **Consumer:** `src/task3/avro_consumer.py`
- Kafka cluster with Schema Registry for AVRO serialization

### Task 4: Spark Processing
- **Job:** `src/task4/spark_streaming.py`
- Processes Kafka streams with transformations
- Outputs to console/file for monitoring

### Task 5: Delta Lake Storage
- **Job:** `src/task5/spark_delta_writer.py`
- Writes processed data to MinIO in Delta Lake format
- **Read Script:** `src/task5/read_delta.py` - Read and query Delta tables

## Services

- **PostgreSQL:** `localhost:5433`
- **Kafka:** `localhost:9092`
- **Schema Registry:** `localhost:8081`
- **Debezium:** `localhost:8083`
- **Spark Master UI:** `http://localhost:8080`
- **MinIO Console:** `http://localhost:9001` (minioadmin/minioadmin)
- **MinIO S3 API:** `http://localhost:9000`

## Documentation

- [Requirements](docs/requirements.md) - Project requirements
- [Deployment Guide](docs/deployment.md) - Complete platform deployment from scratch (Tasks 1-6)
- [Quick Start Guide](docs/quick_start.md) - Detailed setup instructions
- [Requirements Checklist](docs/requirements_checklist.md) - Verification checklist
- [Spark Troubleshooting](docs/spark_troubleshooting.md) - Spark job issues and fixes
- [MinIO Guide](docs/minio_guide.md) - How to retrieve data from MinIO Delta Lake

## Scripts

- `scripts/run_pipeline.sh` / `scripts/run_pipeline.py` - Execute full pipeline
- `scripts/test_setup.sh` - Test and verify setup
- `scripts/run_spark_job.sh` - Run Spark streaming job (Task 4)
- `scripts/run_spark_delta_job.sh` - Run Spark Delta Lake job (Task 5)
- `scripts/run_full_pipeline.sh` - Run full end-to-end pipeline (Tasks 1-5) with background Spark jobs and Delta snapshot

## Utilities

- `src/utils/database.py` - Database cleanup utilities

## Development

1. Activate virtual environment:
   ```bash
   source venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt  # If you create one
   ```

## License

Academic project for data platform course.

