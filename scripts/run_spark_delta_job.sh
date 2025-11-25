#!/bin/bash

# Script to run Spark Delta Lake job (Task 5) with all required dependencies

echo "Preparing Spark Delta Lake job dependencies..."

for container in spark spark-worker; do
  echo "Syncing jars inside ${container}..."
  docker exec "$container" bash -c '
set -euo pipefail
cd /tmp

download_if_missing() {
  local jar_name="$1"
  local jar_url="$2"
  if [ ! -f "/opt/spark/jars/${jar_name}" ]; then
    echo "Downloading ${jar_name}..."
    wget -q "${jar_url}" -O "${jar_name}"
    mv "${jar_name}" /opt/spark/jars/
  fi
}

# Kafka dependencies (same as task4)
download_if_missing "spark-sql-kafka-0-10_2.12-3.5.1.jar" "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar"
download_if_missing "kafka-clients-3.5.1.jar" "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar"
download_if_missing "spark-token-provider-kafka-0-10_2.12-3.5.1.jar" "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar"
download_if_missing "commons-pool2-2.12.0.jar" "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar"

# Delta Lake dependencies
download_if_missing "delta-spark_2.12-3.0.0.jar" "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar"
download_if_missing "delta-storage-3.0.0.jar" "https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar"

# S3/Hadoop AWS dependencies for MinIO
download_if_missing "hadoop-aws-3.3.4.jar" "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
download_if_missing "aws-java-sdk-bundle-1.12.262.jar" "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
'
done

echo 'All dependencies ready in /opt/spark/jars/ on master and worker.'

# Ensure MinIO bucket exists
echo ""
echo "Setting up MinIO bucket..."
docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
docker exec minio mc mb myminio/delta-lake 2>/dev/null || echo "Bucket may already exist"

# Run Spark Delta Lake job
echo ""
echo "Starting Spark Delta Lake job (Task 5)..."
echo "Note: The job will process Kafka messages and write them to MinIO in Delta format."
echo "If no data appears, try inserting data into PostgreSQL to generate CDC events."
echo ""

DELTA_JARS="/opt/spark/jars/delta-spark_2.12-3.0.0.jar,/opt/spark/jars/delta-storage-3.0.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"

docker exec spark /opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --jars "$DELTA_JARS" \
  /opt/spark/apps/spark_delta_writer.py

