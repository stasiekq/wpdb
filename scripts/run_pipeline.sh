#!/bin/bash

# Pipeline Execution Script - Runs all steps and shows raw logs
# This script executes the entire pipeline from Tasks 1-4 and displays what's happening

set -e

echo "========================================="
echo "Mini Data Platform Pipeline Execution"
echo "========================================="
echo ""

# Step 1: Ensure .env exists
echo "[STEP 1] Checking environment configuration..."
if [ ! -f .env ]; then
    echo "Creating .env file..."
    cat > .env << EOF
POSTGRES_USER=pguser
POSTGRES_PASSWORD=admin
POSTGRES_DB=business_db
DB_HOST=localhost
DB_PORT=5433
DATA_DIR=data
EOF
    echo ".env file created"
else
    echo ".env file exists"
fi
echo ""

# Step 2: Start Docker containers
echo "[STEP 2] Starting Docker containers..."
docker-compose up -d
echo "Containers started. Waiting 15 seconds for services to initialize..."
sleep 15
echo ""

# Step 3: Show container status
echo "[STEP 3] Container status:"
docker-compose ps
echo ""

# Step 4: Wait for PostgreSQL and show logs
echo "[STEP 4] Waiting for PostgreSQL to be ready..."
until docker exec pg_task1 pg_isready -U pguser > /dev/null 2>&1; do
    echo -n "."
    sleep 1
done
echo " PostgreSQL is ready"
echo ""
echo "PostgreSQL logs (last 10 lines):"
docker logs pg_task1 --tail 10
echo ""

# Step 5: Wait for Kafka and show logs
echo "[STEP 5] Waiting for Kafka to be ready..."
until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    echo -n "."
    sleep 1
done
echo " Kafka is ready"
echo ""
echo "Kafka logs (last 10 lines):"
docker logs kafka --tail 10
echo ""

# Step 6: Wait for Schema Registry
echo "[STEP 6] Waiting for Schema Registry..."
until curl -s http://localhost:8081/subjects > /dev/null 2>&1; do
    echo -n "."
    sleep 1
done
echo " Schema Registry is ready"
echo ""
echo "Schema Registry logs (last 10 lines):"
docker logs schema-registry --tail 10
echo ""

# Step 7: Wait for Debezium
echo "[STEP 7] Waiting for Debezium..."
until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
    echo -n "."
    sleep 1
done
echo " Debezium is ready"
echo ""
echo "Debezium logs (last 10 lines):"
docker logs debezium --tail 10
echo ""

# Step 8: Wait for MinIO
echo "[STEP 8] Waiting for MinIO..."
until curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; do
    echo -n "."
    sleep 1
done
echo " MinIO is ready"
echo ""
echo "MinIO logs (last 10 lines):"
docker logs minio --tail 10
echo ""

# Step 9: Run Task 1 - Load CSV data
echo "[STEP 9] Executing Task 1: Loading CSV data into PostgreSQL..."
echo "Running: python3 src/task1/csv_loader.py"
echo "----------------------------------------"
python3 src/task1/csv_loader.py
echo "----------------------------------------"
echo "Task 1 completed"
echo ""

# Step 10: Show PostgreSQL tables
echo "[STEP 10] Verifying data in PostgreSQL..."
echo "Tables created:"
docker exec pg_task1 psql -U pguser -d business_db -c "\dt" 2>&1
echo ""
echo "Sample data from data1:"
docker exec pg_task1 psql -U pguser -d business_db -c "SELECT * FROM data1 LIMIT 5;" 2>&1
echo ""

# Step 11: Register Debezium connector
echo "[STEP 11] Registering Debezium PostgreSQL connector..."
echo "POST http://localhost:8083/connectors"
echo "Payload:"
cat config/debezium/postgres_connector.json | python3 -m json.tool
echo ""
echo "Response:"
curl -i -X POST http://localhost:8083/connectors \
  -H "Accept:application/json" \
  -H "Content-Type:application/json" \
  -d @config/debezium/postgres_connector.json
echo ""
echo "Waiting 5 seconds for connector to initialize..."
sleep 5
echo ""

# Step 12: Check connector status
echo "[STEP 12] Debezium connector status:"
curl -s http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool
echo ""

# Step 13: Show Debezium logs after connector registration
echo "Debezium logs after connector registration (last 20 lines):"
docker logs debezium --tail 20
echo ""

# Step 14: Check Kafka topics
echo "[STEP 14] Kafka topics created by Debezium:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
echo ""

# Step 15: Check Schema Registry schemas
echo "[STEP 15] Schema Registry - Registered schemas:"
SCHEMAS=$(curl -s http://localhost:8081/subjects)
if [ -n "$SCHEMAS" ] && [ "$SCHEMAS" != "[]" ]; then
    echo "$SCHEMAS" | python3 -m json.tool
    echo ""
    echo "Schema details (first subject):"
    FIRST_SUBJECT=$(echo "$SCHEMAS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data[0] if data else '')" 2>/dev/null)
    if [ -n "$FIRST_SUBJECT" ]; then
        curl -s "http://localhost:8081/subjects/$FIRST_SUBJECT/versions/latest" | python3 -m json.tool | head -30
    fi
else
    echo "No schemas registered yet (will appear after first CDC event)"
fi
echo ""

# Step 16: Show Kafka message count
echo "[STEP 16] Checking Kafka message counts:"
for topic in pg.public.data1 pg.public.data2 pg.public.data3; do
    COUNT=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic "$topic" 2>/dev/null | awk -F: '{sum += $3} END {print sum+0}' || echo "0")
    echo "  $topic: $COUNT messages"
done
echo ""

# Step 17: Test AVRO consumer (briefly)
echo "[STEP 17] Testing AVRO consumer (5 seconds)..."
echo "Running: python3 src/task3/avro_consumer.py"
echo "----------------------------------------"
timeout 5 python3 src/task3/avro_consumer.py 2>&1 || true
echo "----------------------------------------"
echo ""

# Step 18: Check Spark status
echo "[STEP 18] Spark cluster status:"
if docker ps | grep -q "spark.*Up"; then
    echo "Spark master: Running"
    echo "Spark UI: http://localhost:8080"
    echo ""
    echo "Spark master logs (last 10 lines):"
    docker logs spark --tail 10
    echo ""
    if docker ps | grep -q "spark-worker.*Up"; then
        echo "Spark worker: Running"
        echo "Spark worker logs (last 10 lines):"
        docker logs spark-worker --tail 10
    fi
else
    echo "Spark containers not running"
fi
echo ""

# Step 19: Show current state summary
echo "[STEP 19] Pipeline State Summary:"
echo "----------------------------------------"
echo "PostgreSQL:"
docker exec pg_task1 psql -U pguser -d business_db -c "SELECT COUNT(*) as row_count, 'data1' as table_name FROM data1 UNION ALL SELECT COUNT(*), 'data2' FROM data2 UNION ALL SELECT COUNT(*), 'data3' FROM data3;" 2>&1
echo ""
echo "Kafka Topics:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep "^pg\." | while read topic; do
    echo "  - $topic"
done
echo ""
echo "Debezium Connector:"
CONNECTOR_STATUS=$(curl -s http://localhost:8083/connectors/postgres-connector/status | python3 -c "import sys, json; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "unknown")
echo "  Status: $CONNECTOR_STATUS"
echo ""
echo "Schema Registry:"
SCHEMA_COUNT=$(curl -s http://localhost:8081/subjects | python3 -c "import sys, json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
echo "  Registered schemas: $SCHEMA_COUNT"
echo ""
echo "MinIO:"
if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo "  Status: Healthy"
    echo "  Console: http://localhost:9001 (minioadmin/minioadmin)"
else
    echo "  Status: Unavailable"
fi
echo ""

# Step 20: Instructions for next steps
echo "[STEP 20] Next Steps:"
echo "----------------------------------------"
echo "1. Generate CDC events by inserting data:"
echo "   docker exec -it pg_task1 psql -U pguser -d business_db -c \"INSERT INTO data1 (key, value) VALUES ('test', 'value');\""
echo ""
echo "2. Run Spark streaming job (Task 4) to process stream:"
echo "   ./scripts/run_spark_job.sh"
echo ""
echo "3. Run Spark Delta Lake job (Task 5) to write to MinIO:"
echo "   ./scripts/run_spark_delta_job.sh"
echo ""
echo "4. Monitor Spark UI:"
echo "   http://localhost:8080"
echo ""
echo "5. View MinIO console and Delta data:"
echo "   http://localhost:9001  (minioadmin / minioadmin)"
echo "   docker exec spark /opt/spark/bin/spark-submit --master spark://spark:7077 --packages io.delta:delta-spark_2.12:3.0.0 /opt/spark/apps/read_delta.py"
echo ""
echo "6. View real-time logs:"
echo "   docker logs -f <container-name>"
echo ""

echo "========================================="
echo "Pipeline execution completed!"
echo "========================================="

