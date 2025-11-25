#!/bin/bash

# Script to run Spark job with all required dependencies

echo "Preparing Spark job dependencies..."

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

download_if_missing "spark-sql-kafka-0-10_2.12-3.5.1.jar" "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar"
download_if_missing "kafka-clients-3.5.1.jar" "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar"
download_if_missing "spark-token-provider-kafka-0-10_2.12-3.5.1.jar" "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar"
download_if_missing "commons-pool2-2.12.0.jar" "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar"
'
done

echo 'All dependencies ready in /opt/spark/jars/ on master and worker.'

# Run Spark job (jars are now in the classpath automatically)
echo ""
echo "Starting Spark job..."
echo "Note: The job will process Kafka messages and display them in the console."
echo "If no data appears, try inserting data into PostgreSQL to generate CDC events."
echo ""
docker exec spark /opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  /opt/spark/apps/spark_streaming.py

