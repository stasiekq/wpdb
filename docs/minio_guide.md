# MinIO Guide - Retrieving Data from Delta Lake

## Overview

MinIO is an S3-compatible object store used to store processed data in Delta Lake format. This guide explains how to access and retrieve data stored in MinIO.

## Accessing MinIO

### Web Console

1. **Access the MinIO Console:**
   - URL: http://localhost:9001
   - Username: `minioadmin`
   - Password: `minioadmin`

2. **Browse Buckets:**
   - Navigate to the `delta-lake` bucket
   - Explore the `streaming-data` folder to see Delta Lake files

### Using MinIO Client (mc)

The MinIO client is available in the MinIO container:

```bash
# List buckets
docker exec minio mc ls myminio/

# List objects in delta-lake bucket
docker exec minio mc ls myminio/delta-lake/streaming-data/

# Download a file
docker exec minio mc cp myminio/delta-lake/streaming-data/part-00000-xxx.snappy.parquet /tmp/
```

## Retrieving Data with Spark

### Reading Delta Tables from MinIO

You can read the Delta Lake data using Spark:

```python
from pyspark.sql import SparkSession

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

# Read Delta table
df = spark.read.format("delta").load("s3a://delta-lake/streaming-data")

# Show data
df.show()

# Query specific data
df.filter(df.source_table == "data1").show()

# Count rows
print(f"Total rows: {df.count()}")
```

### Running a Read Script

Create a script `src/task5/read_delta.py`:

```python
from pyspark.sql import SparkSession

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

# Read Delta table
df = spark.read.format("delta").load("s3a://delta-lake/streaming-data")

print("Schema:")
df.printSchema()

print("\nTotal rows:", df.count())

print("\nSample data:")
df.show(20, truncate=False)

print("\nData by source table:")
df.groupBy("source_table").count().show()

spark.stop()
```

Run it:
```bash
docker exec spark /opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --packages io.delta:delta-spark_2.12:3.0.0 \
  /path/to/read_delta.py
```

## Using Python (boto3) for Direct Access

You can also access MinIO using the boto3 library (S3-compatible):

```python
import boto3
from botocore.client import Config

# Create S3 client for MinIO
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4')
)

# List objects
objects = s3_client.list_objects_v2(Bucket='delta-lake', Prefix='streaming-data/')
for obj in objects.get('Contents', []):
    print(f"Key: {obj['Key']}, Size: {obj['Size']}")

# Download a file
s3_client.download_file('delta-lake', 'streaming-data/part-00000-xxx.snappy.parquet', '/tmp/local-file.parquet')
```

## Delta Lake Features

Delta Lake provides several useful features:

### Time Travel (Query Previous Versions)

```python
# Read a specific version
df = spark.read.format("delta").option("versionAsOf", 0).load("s3a://delta-lake/streaming-data")

# Read a specific timestamp
df = spark.read.format("delta").option("timestampAsOf", "2025-11-25 12:00:00").load("s3a://delta-lake/streaming-data")
```

### History

```python
# Show table history
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "s3a://delta-lake/streaming-data")
delta_table.history().show()
```

### Vacuum (Clean Up Old Files)

```python
# Remove files older than 7 days
delta_table.vacuum(retentionHours=168)
```

## Troubleshooting

### Connection Issues

If you can't connect to MinIO:
1. Check if MinIO is running: `docker ps | grep minio`
2. Verify network connectivity: `docker exec spark ping minio`
3. Check MinIO logs: `docker logs minio`

### Bucket Not Found

If the bucket doesn't exist, create it:
```bash
docker exec minio mc mb myminio/delta-lake
```

### Permission Issues

Ensure Spark has the correct S3 credentials configured in the Spark session.

## References

- [MinIO Documentation](https://min.io/docs/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Spark S3 Integration](https://spark.apache.org/docs/latest/cloud-integration.html)

