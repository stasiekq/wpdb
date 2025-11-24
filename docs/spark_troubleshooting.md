# Spark Job - Error Fix and Usage

## Issue Found

When running the Spark job, you may encounter a serialization error with the console output format. This is a known compatibility issue with Spark Structured Streaming.

## Solution

The required JAR files need to be in Spark's classpath. Use the provided script:

```bash
./run_spark_job.sh
```

This script:
1. Downloads required dependencies (if not already present)
2. Copies them to `/opt/spark/jars/` so they're available to all executors
3. Runs the Spark job

## Required Dependencies

The Spark job needs these JAR files:
- `spark-sql-kafka-0-10_2.12-3.5.0.jar` - Kafka connector for Spark
- `kafka-clients-3.5.0.jar` - Kafka client library
- `spark-token-provider-kafka-0-10_2.12-3.5.0.jar` - Token provider for Kafka

## Manual Run (Alternative)

If you prefer to run manually:

```bash
# 1. Ensure JARs are in Spark jars directory
docker exec spark bash -c "
  cd /tmp
  wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar
  wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar
  wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar
  cp *.jar /opt/spark/jars/
"

# 2. Run the job
docker exec spark /opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  /opt/spark/apps/spark_job.py
```

## What the Job Does

1. **Reads from Kafka**: Connects to Kafka topics `pg.public.data1`, `pg.public.data2`, `pg.public.data3`
2. **Performs Transformations**:
   - Extracts source table name from topic
   - Adds `is_primary_partition` flag
   - Calculates `message_age_ms`
   - Adds `message_sequence` (offset)
   - Adds `processing_timestamp`
3. **Outputs to Console**: Displays transformed data (ready for Task 5 to write to MinIO)

## Generating Test Data

To see the job process data, insert records into PostgreSQL:

```bash
docker exec -it pg_task1 psql -U pguser -d business_db -c "INSERT INTO data1 (key, value) VALUES ('test_key', 'test_value');"
```

This will generate a CDC event that Debezium captures and sends to Kafka, which Spark will then process.

## Monitoring

- **Spark UI**: http://localhost:8080
- **Check running applications**: The job will appear in the Spark UI
- **View logs**: Check Spark UI or container logs

## Known Issues

- Console output format may have serialization issues in some Spark versions
- If you see serialization errors, the job is still reading from Kafka correctly
- For production (Task 5), you'll write to MinIO/Delta format which doesn't have this issue


