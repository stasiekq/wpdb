"""
Spark Structured Streaming Job for Task 5
Reads AVRO messages from Kafka, performs transformations, and writes to MinIO in Delta format
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, unix_timestamp, split
from pyspark.sql.types import StructType
import os

# Initialize Spark Session with Delta Lake and S3 (MinIO) support
spark = SparkSession.builder \
    .appName("KafkaToDeltaMinIO") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka configuration
kafka_bootstrap_servers = "kafka:9092"
kafka_topics = "pg.public.data1,pg.public.data2,pg.public.data3"

# MinIO/S3 configuration
minio_bucket = "delta-lake"
minio_path = f"s3a://{minio_bucket}/streaming-data"

print("Starting Spark Structured Streaming from Kafka to MinIO (Delta)...")
print(f"Kafka server: {kafka_bootstrap_servers}")
print(f"Topics: {kafka_topics}")
print(f"MinIO path: {minio_path}")

# Read from Kafka using Structured Streaming
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topics) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

print("Kafka stream connected. Raw schema:")
df.printSchema()

# Extract basic Kafka metadata
stream_df = df.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("avro_value_hex"),
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp")
)

# Perform transformations
transformed_df = stream_df.select(
    col("kafka_key"),
    col("kafka_topic"),
    col("kafka_partition"),
    col("kafka_offset"),
    col("kafka_timestamp"),
    col("avro_value_hex"),
    current_timestamp().alias("processing_timestamp")
)

# Transformation 1: Extract table name from topic (Debezium format: pg.public.tablename)
transformed_df = transformed_df.withColumn(
    "source_table",
    split(col("kafka_topic"), "\\.")[2]  # Split by "." and take 3rd element (index 2: data1, data2, data3)
)

# Transformation 2: Add partition flag
transformed_df = transformed_df.withColumn(
    "is_primary_partition",
    (col("kafka_partition") == 0).cast("boolean")
)

# Transformation 3: Calculate message age
transformed_df = transformed_df.withColumn(
    "message_age_ms",
    (unix_timestamp(col("processing_timestamp")) * 1000) - (unix_timestamp(col("kafka_timestamp")) * 1000)
)

# Transformation 4: Add message sequence
transformed_df = transformed_df.withColumn(
    "message_sequence",
    col("kafka_offset")
)

print("Transformed schema:")
transformed_df.printSchema()

# Write to MinIO in Delta format
print(f"\nWriting to MinIO Delta Lake at: {minio_path}")
print("Note: Ensure MinIO bucket 'delta-lake' exists and is accessible")

# Use foreachBatch to write to Delta format
# Store minio_path as a module-level variable to avoid closure serialization issues
_minio_path = minio_path

def write_to_delta(batch_df, batch_id):
    """Write each batch to Delta format in MinIO"""
    try:
        row_count = batch_df.count()
        if row_count > 0:
            # Write directly to Delta format
            batch_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(_minio_path)
            print(f"Batch {batch_id}: Successfully wrote {row_count} rows to Delta Lake")
        else:
            print(f"Batch {batch_id}: No new data to write")
    except Exception as e:
        print(f"Error writing batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()
        # Don't raise - let streaming continue
        pass

# Use a local checkpoint location (not in S3) to avoid serialization issues
checkpoint_path = "/tmp/spark-checkpoints/delta-writer"

query = transformed_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_delta) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming query started. Writing to Delta format in MinIO every 10 seconds...")
print("Data will be stored in Delta Lake format at:", minio_path)
print("Press Ctrl+C to stop.")

# Wait for the streaming query to finish
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping streaming query...")
    query.stop()
    print("Query stopped. Data written to MinIO.")

