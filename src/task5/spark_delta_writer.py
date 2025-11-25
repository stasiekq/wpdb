"""
Spark Structured Streaming Job for Task 5
Reads AVRO messages from Kafka, performs transformations, and writes to MinIO in Delta format
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, unix_timestamp
from pyspark.sql.types import StructType
import os

# Initialize Spark Session with S3 support but WITHOUT Delta extension/catalog to avoid serialization issues
# We'll configure Delta only for writes in foreachBatch using format("delta")
# Delta Lake JARs are provided via --jars in spark-submit command
# Note: NOT setting Delta extension/catalog here to avoid serialization issues during read phase
spark = SparkSession.builder \
    .appName("KafkaToDeltaMinIO") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/delta-spark_2.12-3.0.0.jar:/opt/spark/jars/delta-storage-3.0.0.jar:/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/delta-spark_2.12-3.0.0.jar:/opt/spark/jars/delta-storage-3.0.0.jar:/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
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

# Transformation 1: Extract table name from topic
transformed_df = transformed_df.withColumn(
    "source_table",
    col("kafka_topic").substr(10, 100)  # Extract after "pg.public."
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

# Use foreachBatch to avoid serialization issues with direct Delta streaming
# Store minio_path as a module-level variable to avoid closure serialization issues
_minio_path = minio_path

def write_to_delta(batch_df, batch_id):
    """Write each batch to Delta format using workaround for serialization issues"""
    try:
        # Get SparkSession from DataFrame
        spark_session = batch_df.sql_ctx.sparkSession
        
        # Workaround: Write to temporary Parquet location first, then convert to Delta
        # This avoids the serialization issue with Delta Lake 3.0.0 + Spark 3.5.0
        temp_parquet_path = f"{_minio_path}/_temp_parquet/batch_{batch_id}"
        
        # Write to Parquet first (no Delta involved, avoids serialization issue)
        batch_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(temp_parquet_path)
        
        # Now configure Delta and convert Parquet to Delta
        spark_session.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        spark_session.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # Read the Parquet data and write as Delta
        temp_df = spark_session.read.format("parquet").load(temp_parquet_path)
        temp_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(_minio_path)
        
        # Clean up temp Parquet files
        try:
            from pyspark.sql import SparkSession
            fs = spark_session._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark_session._jsc.hadoopConfiguration()
            )
            temp_path = spark_session._jvm.org.apache.hadoop.fs.Path(temp_parquet_path)
            if fs.exists(temp_path):
                fs.delete(temp_path, True)
        except:
            pass  # Ignore cleanup errors
        
        print(f"Batch {batch_id} written successfully to Delta Lake (via Parquet workaround)")
    except Exception as e:
        print(f"Error writing batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()
        # Don't raise - let streaming continue to see if subsequent batches work
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

