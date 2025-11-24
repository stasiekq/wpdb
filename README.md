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

3. **Run Spark job:**
   ```bash
   ./scripts/run_spark_job.sh
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

## Services

- **PostgreSQL:** `localhost:5433`
- **Kafka:** `localhost:9092`
- **Schema Registry:** `localhost:8081`
- **Debezium:** `localhost:8083`
- **Spark Master UI:** `http://localhost:8080`

## Documentation

- [Requirements](docs/requirements.md) - Project requirements
- [Quick Start Guide](docs/quick_start.md) - Detailed setup instructions
- [Requirements Checklist](docs/requirements_checklist.md) - Verification checklist
- [Spark Troubleshooting](docs/spark_troubleshooting.md) - Spark job issues and fixes

## Scripts

- `scripts/run_pipeline.sh` / `scripts/run_pipeline.py` - Execute full pipeline
- `scripts/test_setup.sh` - Test and verify setup
- `scripts/run_spark_job.sh` - Run Spark streaming job

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

