#!/bin/bash

# Script to run Spark job with all required dependencies

echo "Preparing Spark job dependencies..."

# Download and copy required JARs to Spark jars directory
docker exec spark bash -c "
cd /tmp
if [ ! -f /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar ]; then
    echo 'Downloading spark-sql-kafka-0-10 connector...'
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar
    cp spark-sql-kafka-0-10_2.12-3.5.0.jar /opt/spark/jars/
fi
if [ ! -f /opt/spark/jars/kafka-clients-3.5.0.jar ]; then
    echo 'Downloading kafka-clients...'
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar
    cp kafka-clients-3.5.0.jar /opt/spark/jars/
fi
if [ ! -f /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar ]; then
    echo 'Downloading spark-token-provider-kafka-0-10...'
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar
    cp spark-token-provider-kafka-0-10_2.12-3.5.0.jar /opt/spark/jars/
fi
echo 'All dependencies ready in /opt/spark/jars/'
"

# Run Spark job (jars are now in the classpath automatically)
echo ""
echo "Starting Spark job..."
echo "Note: The job will process Kafka messages and display them in the console."
echo "If no data appears, try inserting data into PostgreSQL to generate CDC events."
echo ""
docker exec spark /opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  /opt/spark/apps/spark_streaming.py

