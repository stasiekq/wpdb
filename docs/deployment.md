# Deployment Guide - Complete Platform Setup

This guide covers deploying the entire data platform from scratch, including all tasks (1-6).

## Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ (for local scripts)
- At least 4GB RAM available for containers
- Ports available: 5433, 8080, 8081, 8082, 8083, 9000, 9001, 9092

## Step 1: Environment Setup

1. **Clone or navigate to the project directory:**
   ```bash
   cd /path/to/wpdb
   ```

2. **Create `.env` file:**
   ```bash
   cat > .env << EOF
   POSTGRES_USER=pguser
   POSTGRES_PASSWORD=admin
   POSTGRES_DB=business_db
   DB_HOST=localhost
   DB_PORT=5433
   DATA_DIR=data
   EOF
   ```

## Step 2: Start All Services

**Option A: Using the automated pipeline script (Recommended):**
```bash
./scripts/run_pipeline.sh
```

This script will:
- Start all Docker containers
- Wait for services to be ready
- Execute Task 1 (load CSV data)
- Configure Debezium connector
- Verify all services

**Option B: Manual deployment:**
```bash
# Start all containers
docker compose up -d

# Wait for services to initialize (30-60 seconds)
docker compose ps

# Verify services are healthy
docker exec pg_task1 pg_isready -U pguser
curl http://localhost:8081/subjects  # Schema Registry
curl http://localhost:8083/connectors  # Debezium
```

## Step 3: Verify Services

### Service URLs and Access

| Service | URL | Credentials |
|---------|-----|-------------|
| PostgreSQL | `localhost:5433` | pguser/admin |
| Kafka | `localhost:9092` | - |
| Schema Registry | `http://localhost:8081` | - |
| Debezium Connect | `http://localhost:8083` | - |
| Spark Master UI | `http://localhost:8080` | - |
| Spark Worker UI | `http://localhost:8082` | - |
| MinIO Console | `http://localhost:9001` | minioadmin/minioadmin |
| MinIO S3 API | `http://localhost:9000` | minioadmin/minioadmin |

### Health Checks

```bash
# PostgreSQL
docker exec pg_task1 pg_isready -U pguser

# Kafka
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Schema Registry
curl http://localhost:8081/subjects

# Debezium
curl http://localhost:8083/connectors

# MinIO
curl http://localhost:9000/minio/health/live
```

## Step 4: Run Tasks

### Task 1: Load CSV Data (if not done by pipeline script)

```bash
python3 src/task1/csv_loader.py
```

### Task 2: Configure Debezium (if not done by pipeline script)

```bash
curl -i -X POST http://localhost:8083/connectors \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  -d @config/debezium/postgres_connector.json
```

### Task 3: Test Kafka Consumer

```bash
python3 src/task3/avro_consumer.py
```

### Task 4: Run Spark Streaming Job

```bash
./scripts/run_spark_job.sh
```

This will:
- Download required Kafka dependencies
- Start Spark streaming job
- Process Kafka messages every 5 seconds
- Write output to `/tmp/spark-streaming-output.txt`

Monitor output:
```bash
docker exec spark tail -f /tmp/spark-streaming-output.txt
```

### Task 5: Run Spark Delta Lake Job

```bash
./scripts/run_spark_delta_job.sh
```

This will:
- Download Delta Lake and S3 dependencies
- Create MinIO bucket if needed
- Start Spark streaming job writing to MinIO in Delta format
- Process Kafka messages every 10 seconds

### Task 6: Verify Complete Pipeline

1. **Insert test data:**
   ```bash
   docker exec -it pg_task1 psql -U pguser -d business_db \
     -c "INSERT INTO data1 (key, value) VALUES ('test', 'value');"
   ```

2. **Verify data flow:**
   - Check Kafka topics have messages
   - Check Spark is processing (Task 4 output file or Task 5 MinIO)
   - Check MinIO console for Delta files

3. **Read Delta data:**
   ```bash
   docker exec spark /opt/spark/bin/spark-submit \
     --master spark://spark:7077 \
     --packages io.delta:delta-spark_2.12:3.0.0 \
     /opt/spark/apps/read_delta.py
   ```

## Data Persistence

All data is persisted in Docker volumes:

- **PostgreSQL data:** `pgdata` volume
- **MinIO data:** `minio_data` volume

To backup:
```bash
docker run --rm -v wpdb_pgdata:/data -v $(pwd):/backup alpine tar czf /backup/pgdata-backup.tar.gz /data
docker run --rm -v wpdb_minio_data:/data -v $(pwd):/backup alpine tar czf /backup/minio-backup.tar.gz /data
```

To restore:
```bash
docker run --rm -v wpdb_pgdata:/data -v $(pwd):/backup alpine tar xzf /backup/pgdata-backup.tar.gz -C /
```

## Troubleshooting

### Services Not Starting

1. **Check container logs:**
   ```bash
   docker compose logs <service-name>
   ```

2. **Check port conflicts:**
   ```bash
   # Check if ports are in use
   lsof -i :5433
   lsof -i :9092
   lsof -i :8080
   ```

3. **Restart services:**
   ```bash
   docker compose restart <service-name>
   ```

### Spark Job Issues

- **Worker not registered:** Check if `spark-worker` container is running
- **Serialization errors:** Already fixed in Task 4 - ensure using latest code
- **Missing dependencies:** Run the job script which downloads dependencies automatically

### MinIO Issues

- **Bucket not found:** The script creates it automatically, or manually:
  ```bash
  docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin
  docker exec minio mc mb myminio/delta-lake
  ```

- **Connection refused:** Check MinIO is running and accessible:
  ```bash
  docker exec spark ping minio
  ```

## Clean Deployment (Fresh Start)

To completely reset and start fresh:

```bash
# Stop all containers
docker compose down

# Remove volumes (WARNING: deletes all data)
docker compose down -v

# Remove images (optional)
docker compose down --rmi all

# Start fresh
docker compose up -d
./scripts/run_pipeline.sh
```

## Production Considerations

For production deployment, consider:

1. **Security:**
   - Change default passwords in `.env`
   - Use Docker secrets for sensitive data
   - Enable SSL/TLS for MinIO
   - Restrict network access

2. **Resource Limits:**
   ```yaml
   services:
     spark-worker:
       deploy:
         resources:
           limits:
             cpus: '2'
             memory: 4G
   ```

3. **Monitoring:**
   - Add Prometheus/Grafana for metrics
   - Set up log aggregation
   - Monitor disk usage for volumes

4. **Backup Strategy:**
   - Regular PostgreSQL dumps
   - MinIO data replication
   - Kafka topic backups

5. **High Availability:**
   - Multiple Kafka brokers
   - Spark worker scaling
   - MinIO distributed mode

## Next Steps

- Review [MinIO Guide](minio_guide.md) for data retrieval
- Check [Spark Troubleshooting](spark_troubleshooting.md) for common issues
- See [Requirements Checklist](requirements_checklist.md) to verify completion

