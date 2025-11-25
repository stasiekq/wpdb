"""
Script to read and display Delta Lake data from MinIO
"""

from pyspark.sql import SparkSession

# Initialize Spark Session with Delta Lake and S3 (MinIO) support
spark = SparkSession.builder \
    .appName("ReadDeltaFromMinIO") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

minio_path = "s3a://delta-lake/streaming-data"

print(f"Reading Delta Lake data from: {minio_path}")

try:
    # Read Delta table
    df = spark.read.format("delta").load(minio_path)
    
    print("\nSchema:")
    df.printSchema()
    
    print(f"\nTotal rows: {df.count()}")
    
    print("\nSample data (first 20 rows):")
    df.show(20, truncate=False)
    
    print("\nData grouped by source table:")
    df.groupBy("source_table").count().orderBy("source_table").show()
    
    print("\nData grouped by kafka_topic:")
    df.groupBy("kafka_topic").count().orderBy("kafka_topic").show()
    
except Exception as e:
    print(f"Error reading Delta table: {e}")
    print("Make sure:")
    print("1. MinIO is running and accessible")
    print("2. The delta-lake bucket exists")
    print("3. Data has been written to the streaming-data path")
    import traceback
    traceback.print_exc()

spark.stop()

