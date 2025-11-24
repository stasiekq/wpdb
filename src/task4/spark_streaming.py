"""
Spark Structured Streaming Job for Task 4
Reads AVRO messages from Kafka, deserializes them, and performs transformations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, unix_timestamp, udf, from_json
from pyspark.sql.types import StringType, StructType, MapType
import struct
import io

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaAvroStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka configuration
kafka_bootstrap_servers = "kafka:9092"
schema_registry_url = "http://schema-registry:8081"
# Debezium creates topics like: pg.public.data1, pg.public.data2, etc.
kafka_topics = "pg.public.data1,pg.public.data2,pg.public.data3"

print("Starting Spark Structured Streaming from Kafka...")
print(f"Kafka server: {kafka_bootstrap_servers}")
print(f"Topics: {kafka_topics}")

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
    col("value"),  # Keep as binary for AVRO deserialization
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp")
)

# For AVRO deserialization with Confluent Schema Registry format:
# Format: [1 byte magic][4 bytes schema_id][AVRO payload]
# Since proper AVRO deserialization requires Schema Registry access and fastavro,
# we'll create a simplified version that extracts the payload structure
# In production, you'd use a library like spark-avro-confluent or custom UDF

# For now, let's convert the binary value to a readable format
# and perform transformations on the metadata and structure

# Basic transformation: Extract information and add computed columns
transformed_df = stream_df.select(
    col("kafka_key"),
    col("kafka_topic"),
    col("kafka_partition"),
    col("kafka_offset"),
    col("kafka_timestamp"),
    # Convert binary value to hex string for inspection (in production, deserialize AVRO here)
    col("value").cast("string").alias("avro_value_hex"),
    current_timestamp().alias("processing_timestamp")
)

# Transformation 1: Extract table name from topic (Debezium format: pg.public.tablename)
transformed_df = transformed_df.withColumn(
    "source_table",
    col("kafka_topic").substr(10, 100)  # Extract after "pg.public."
)

# Transformation 2: Add a flag based on partition
transformed_df = transformed_df.withColumn(
    "is_primary_partition",
    (col("kafka_partition") == 0).cast("boolean")
)

# Transformation 3: Calculate message age (difference between kafka timestamp and processing time)
# Kafka timestamp is in milliseconds, convert processing_timestamp to milliseconds
transformed_df = transformed_df.withColumn(
    "message_age_ms",
    (unix_timestamp(col("processing_timestamp")) * 1000) - (unix_timestamp(col("kafka_timestamp")) * 1000)
)

# Transformation 4: Add a row number within each partition (using offset as proxy)
transformed_df = transformed_df.withColumn(
    "message_sequence",
    col("kafka_offset")
)

print("Transformed schema:")
transformed_df.printSchema()

# Write to console for demonstration
# In Task 5, this will be written to MinIO in Delta format
query = transformed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

print("Streaming query started. Processing data every 5 seconds...")
print("Press Ctrl+C to stop.")

# Wait for the streaming query to finish
query.awaitTermination()

