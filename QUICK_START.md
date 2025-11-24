# Quick Start Guide - Testing Tasks 1-4

## Option 1: Automated Test (Recommended)

Run the test script:
```bash
./test_setup.sh
```

## Option 2: Manual Step-by-Step

### 1. Create .env file (if it doesn't exist)
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

### 2. Start all Docker containers
```bash
docker-compose up -d
```

Wait for services to be ready (about 30-60 seconds):
```bash
# Check status
docker-compose ps

# Wait for PostgreSQL
docker exec pg_task1 pg_isready -U pguser

# Wait for Kafka
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Wait for Schema Registry
curl http://localhost:8081/subjects

# Wait for Debezium
curl http://localhost:8083/connectors
```

### 3. Run Task 1 - Load CSV data into PostgreSQL
```bash
python3 business_project_task_1.py
```

This will:
- Read `data/data1.csv`, `data/data2.csv`, `data/data3.csv`
- Create tables `data1`, `data2`, `data3` in PostgreSQL
- Insert the data

### 4. Register Debezium Connector
```bash
curl -i -X POST http://localhost:8083/connectors \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  -d @register_postgres.json
```

Verify it's running:
```bash
curl http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool
```

### 5. Verify Kafka Topics Created
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

You should see topics like:
- `pg.public.data1`
- `pg.public.data2`
- `pg.public.data3`

### 6. Test AVRO Consumer (Task 3)
```bash
python3 consumer_avro.py
```

This will read and decode AVRO messages from Kafka. Press Ctrl+C to stop.

### 7. Generate More CDC Events (Optional)
To see streaming in action, insert more data:
```bash
docker exec -it pg_task1 psql -U pguser -d business_db -c "INSERT INTO data1 (key, value) VALUES ('test', 'value');"
```

### 8. Run Spark Job (Task 4)

**Option A: Submit to Spark cluster**
```bash
docker exec -it spark /opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/apps/spark_job.py
```

**Option B: Run locally (if you have Spark installed)**
```bash
python3 spark_job.py
```

**Option C: Access Spark UI**
Open browser: http://localhost:8080

The Spark job will:
- Read AVRO messages from Kafka topics
- Perform transformations (extract table names, add computed columns)
- Write results to console (in Task 5, this will go to MinIO)

## Troubleshooting

### Check container logs
```bash
docker-compose logs postgres
docker-compose logs kafka
docker-compose logs schema-registry
docker-compose logs debezium
docker-compose logs spark
```

### Restart a service
```bash
docker-compose restart <service-name>
```

### Stop everything
```bash
docker-compose down
```

### Clean start (removes volumes)
```bash
docker-compose down -v
docker-compose up -d
```

## Verify Everything Works

1. ✅ PostgreSQL has data: `docker exec -it pg_task1 psql -U pguser -d business_db -c "SELECT * FROM data1 LIMIT 5;"`
2. ✅ Kafka has topics: `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list`
3. ✅ Schema Registry has schemas: `curl http://localhost:8081/subjects`
4. ✅ Debezium connector is running: `curl http://localhost:8083/connectors/postgres-connector/status`
5. ✅ Spark UI is accessible: http://localhost:8080

