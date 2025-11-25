#!/bin/bash

# Full pipeline orchestrator (Tasks 1-5)
# - Runs run_pipeline.sh (Tasks 1-4)
# - Seeds CDC data automatically
# - Starts Spark streaming and Delta jobs in background
# - Shows a Delta snapshot via read_delta.py

set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
STREAM_LOG="${LOG_DIR}/spark_streaming.log"
DELTA_LOG="${LOG_DIR}/spark_delta.log"

mkdir -p "${LOG_DIR}"

echo "========================================="
echo "Running full data platform pipeline"
echo "========================================="
echo ""

pushd "${ROOT_DIR}" >/dev/null

echo "[1/7] Running base pipeline (Tasks 1-4)..."
./scripts/run_pipeline.sh
echo ""

echo "[2/7] Seeding CDC data in PostgreSQL..."
SEED_TS=$(date +%s)
for table in data1 data2 data3; do
  docker exec pg_task1 psql -U pguser -d business_db \
    -c "INSERT INTO ${table} (key, value) VALUES ('seed_${table}_${SEED_TS}', 'value_${SEED_TS}');" >/dev/null
done
echo "Seed rows inserted (timestamp: ${SEED_TS})."
echo ""

echo "[3/7] Starting Spark streaming job (Task 4) in background..."
./scripts/run_spark_job.sh > "${STREAM_LOG}" 2>&1 &
STREAM_PID=$!
echo "Spark streaming job started (PID ${STREAM_PID}). Logs: ${STREAM_LOG}"
echo ""

echo "[4/7] Starting Spark Delta Lake job (Task 5) in background..."
./scripts/run_spark_delta_job.sh > "${DELTA_LOG}" 2>&1 &
DELTA_PID=$!
echo "Spark Delta job started (PID ${DELTA_PID}). Logs: ${DELTA_LOG}"
echo ""

echo "[5/7] Waiting 20 seconds for streaming jobs to ingest data..."
sleep 20
echo ""

echo "[6/7] Reading snapshot from Delta Lake..."
DELTA_JARS="/opt/spark/jars/delta-spark_2.12-3.0.0.jar,/opt/spark/jars/delta-storage-3.0.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
docker exec spark /opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --jars "${DELTA_JARS}" \
  /opt/spark/apps/read_delta.py || true
echo ""

echo "[7/7] Pipeline summary"
echo "-----------------------------------------"
echo "Spark streaming job PID : ${STREAM_PID}"
echo "Spark delta job PID     : ${DELTA_PID}"
echo "Streaming log           : ${STREAM_LOG}"
echo "Delta log               : ${DELTA_LOG}"
echo ""
echo "Stop jobs with:"
echo "  kill ${STREAM_PID} ${DELTA_PID}"
echo ""
echo "View logs with:"
echo "  tail -f ${STREAM_LOG}"
echo "  tail -f ${DELTA_LOG}"
echo ""
echo "Delta Lake data location: s3a://delta-lake/streaming-data"
echo "MinIO console           : http://localhost:9001 (minioadmin/minioadmin)"
echo ""
echo "========================================="
echo "Full pipeline started. Jobs running in background."
echo "Use Ctrl+C to exit this script (jobs keep running)."
echo "========================================="

popd >/dev/null

