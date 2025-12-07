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
# Download Delta dependencies first
echo "Preparing Delta Lake dependencies..."
for container in spark spark-worker; do
  docker exec "$container" bash -c '
    cd /tmp
    download_if_missing() {
      local jar_name="$1"
      local jar_url="$2"
      if [ ! -f "/opt/spark/jars/${jar_name}" ]; then
        echo "Downloading ${jar_name}..."
        wget -q "${jar_url}" -O "${jar_name}" && mv "${jar_name}" /opt/spark/jars/ || echo "Failed"
      fi
    }
    download_if_missing "delta-spark_2.12-3.0.0.jar" "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar"
    download_if_missing "delta-storage-3.0.0.jar" "https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar"
    download_if_missing "hadoop-aws-3.3.4.jar" "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    download_if_missing "aws-java-sdk-bundle-1.12.262.jar" "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
  ' >/dev/null 2>&1
done

# Ensure MinIO bucket exists
docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
docker exec minio mc mb myminio/delta-lake 2>/dev/null || true

# Run Delta job in local mode to avoid resource contention with streaming job
DELTA_JARS="/opt/spark/jars/delta-spark_2.12-3.0.0.jar,/opt/spark/jars/delta-storage-3.0.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
docker exec spark /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --jars "${DELTA_JARS}" \
  /opt/spark/apps/spark_delta_writer.py > "${DELTA_LOG}" 2>&1 &
DELTA_PID=$!
echo "Spark Delta job started in local mode (PID ${DELTA_PID}). Logs: ${DELTA_LOG}"
echo ""

echo "[5/7] Waiting 15 seconds for streaming jobs to ingest data..."
sleep 15
echo ""

echo "[6/7] Ensuring Delta Lake dependencies are available..."
# Download Delta jars to spark container if not present
docker exec spark bash -c '
cd /tmp
download_if_missing() {
  local jar_name="$1"
  local jar_url="$2"
  if [ ! -f "/opt/spark/jars/${jar_name}" ]; then
    echo "Downloading ${jar_name}..."
    wget -q "${jar_url}" -O "${jar_name}" && mv "${jar_name}" /opt/spark/jars/ || echo "Failed to download ${jar_name}"
  fi
}

download_if_missing "delta-spark_2.12-3.0.0.jar" "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar"
download_if_missing "delta-storage-3.0.0.jar" "https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar"
download_if_missing "hadoop-aws-3.3.4.jar" "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
download_if_missing "aws-java-sdk-bundle-1.12.262.jar" "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
' || true
echo ""

echo "[7/8] Reading snapshot from Delta Lake..."
# Build jar list - AWS SDK bundle is required by hadoop-aws
DELTA_JARS="/opt/spark/jars/delta-spark_2.12-3.0.0.jar,/opt/spark/jars/delta-storage-3.0.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"

# Using local[*] to avoid competing for cluster resources with background jobs
# Note: If data doesn't exist yet, the script will show a helpful error message
docker exec spark /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --jars "${DELTA_JARS}" \
  /opt/spark/apps/read_delta.py 2>&1 | grep -v "WARN MetricsConfig" || {
  echo ""
  echo "Note: If Delta data doesn't exist yet, the Delta job may still be processing."
  echo "Check the Delta log: ${DELTA_LOG}"
  echo "Or wait a bit longer and run manually:"
  echo "  docker exec spark /opt/spark/bin/spark-submit --master 'local[*]' --jars \"${DELTA_JARS}\" /opt/spark/apps/read_delta.py"
}
echo ""

echo "[8/8] Pipeline summary"
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

