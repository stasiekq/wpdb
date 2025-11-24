#!/usr/bin/env python3
"""
Pipeline Execution Script - Runs all steps and shows raw logs
This script executes the entire pipeline from Tasks 1-4 and displays what's happening
"""

import subprocess
import time
import json
import os
import sys
from pathlib import Path


def run_command(cmd, shell=False, check=False, capture_output=False):
    """Run a command and return the result"""
    if isinstance(cmd, str) and not shell:
        cmd = cmd.split()
    
    result = subprocess.run(
        cmd,
        shell=shell,
        check=check,
        capture_output=capture_output,
        text=True
    )
    
    if capture_output:
        return result.stdout, result.stderr, result.returncode
    return result.returncode


def print_step(step_num, title):
    """Print a step header"""
    print(f"\n[STEP {step_num}] {title}")
    print("-" * 50)


def wait_for_service(check_cmd, service_name, max_wait=60, shell=False):
    """Wait for a service to be ready"""
    print(f"Waiting for {service_name} to be ready...", end="", flush=True)
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        result = run_command(check_cmd, shell=shell, capture_output=True)
        if result[2] == 0:
            print(f" {service_name} is ready")
            return True
        print(".", end="", flush=True)
        time.sleep(1)
    
    print(f" {service_name} timeout")
    return False


def get_docker_logs(container, lines=10):
    """Get docker logs for a container"""
    result = run_command(
        f"docker logs {container} --tail {lines}",
        shell=True,
        capture_output=True
    )
    return result[0]


def main():
    print("=" * 50)
    print("Mini Data Platform Pipeline Execution")
    print("=" * 50)
    print()
    
    # Step 1: Ensure .env exists
    print_step(1, "Checking environment configuration")
    env_file = Path(".env")
    if not env_file.exists():
        print("Creating .env file...")
        env_content = """POSTGRES_USER=pguser
POSTGRES_PASSWORD=admin
POSTGRES_DB=business_db
DB_HOST=localhost
DB_PORT=5433
DATA_DIR=data
"""
        env_file.write_text(env_content)
        print(".env file created")
    else:
        print(".env file exists")
    print()
    
    # Step 2: Start Docker containers
    print_step(2, "Starting Docker containers")
    run_command("docker-compose up -d", shell=True)
    print("Containers started. Waiting 15 seconds for services to initialize...")
    time.sleep(15)
    print()
    
    # Step 3: Show container status
    print_step(3, "Container status")
    run_command("docker-compose ps", shell=True)
    print()
    
    # Step 4: Wait for PostgreSQL
    print_step(4, "Waiting for PostgreSQL to be ready")
    if wait_for_service(
        "docker exec pg_task1 pg_isready -U pguser",
        "PostgreSQL",
        shell=True
    ):
        print("\nPostgreSQL logs (last 10 lines):")
        print(get_docker_logs("pg_task1", 10))
    print()
    
    # Step 5: Wait for Kafka
    print_step(5, "Waiting for Kafka to be ready")
    if wait_for_service(
        "docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092",
        "Kafka",
        shell=True
    ):
        print("\nKafka logs (last 10 lines):")
        print(get_docker_logs("kafka", 10))
    print()
    
    # Step 6: Wait for Schema Registry
    print_step(6, "Waiting for Schema Registry")
    if wait_for_service(
        "curl -s http://localhost:8081/subjects",
        "Schema Registry",
        shell=True
    ):
        print("\nSchema Registry logs (last 10 lines):")
        print(get_docker_logs("schema-registry", 10))
    print()
    
    # Step 7: Wait for Debezium
    print_step(7, "Waiting for Debezium")
    if wait_for_service(
        "curl -s http://localhost:8083/connectors",
        "Debezium",
        shell=True
    ):
        print("\nDebezium logs (last 10 lines):")
        print(get_docker_logs("debezium", 10))
    print()
    
    # Step 8: Run Task 1 - Load CSV data
    print_step(8, "Executing Task 1: Loading CSV data into PostgreSQL")
    print("Running: python3 src/task1/csv_loader.py")
    print("-" * 50)
    result = run_command(["python3", "src/task1/csv_loader.py"])
    if result != 0:
        print(f"ERROR: Task 1 failed with exit code {result}")
        sys.exit(1)
    print("-" * 50)
    print("Task 1 completed")
    print()
    
    # Step 9: Show PostgreSQL tables
    print_step(9, "Verifying data in PostgreSQL")
    print("Tables created:")
    run_command(
        'docker exec pg_task1 psql -U pguser -d business_db -c "\\dt"',
        shell=True
    )
    print()
    print("Sample data from data1:")
    run_command(
        'docker exec pg_task1 psql -U pguser -d business_db -c "SELECT * FROM data1 LIMIT 5;"',
        shell=True
    )
    print()
    
    # Step 10: Register Debezium connector
    print_step(10, "Registering Debezium PostgreSQL connector")
    print("POST http://localhost:8083/connectors")
    print("Payload:")
    with open("config/debezium/postgres_connector.json", "r") as f:
        payload = json.load(f)
        print(json.dumps(payload, indent=2))
    print()
    print("Response:")
    run_command(
        'curl -i -X POST http://localhost:8083/connectors '
        '-H "Accept:application/json" '
        '-H "Content-Type:application/json" '
        '-d @config/debezium/postgres_connector.json',
        shell=True
    )
    print()
    print("Waiting 5 seconds for connector to initialize...")
    time.sleep(5)
    print()
    
    # Step 11: Check connector status
    print_step(11, "Debezium connector status")
    result = run_command(
        "curl -s http://localhost:8083/connectors/postgres-connector/status",
        shell=True,
        capture_output=True
    )
    if result[0]:
        try:
            status = json.loads(result[0])
            print(json.dumps(status, indent=2))
        except json.JSONDecodeError:
            print(result[0])
    print()
    
    # Step 12: Show Debezium logs after connector registration
    print("Debezium logs after connector registration (last 20 lines):")
    print(get_docker_logs("debezium", 20))
    print()
    
    # Step 13: Check Kafka topics
    print_step(13, "Kafka topics created by Debezium")
    run_command(
        "docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list",
        shell=True
    )
    print()
    
    # Step 14: Check Schema Registry schemas
    print_step(14, "Schema Registry - Registered schemas")
    result = run_command(
        "curl -s http://localhost:8081/subjects",
        shell=True,
        capture_output=True
    )
    if result[0] and result[0].strip() and result[0].strip() != "[]":
        try:
            schemas = json.loads(result[0])
            print(json.dumps(schemas, indent=2))
            print()
            if schemas:
                print("Schema details (first subject):")
                first_subject = schemas[0]
                schema_result = run_command(
                    f"curl -s http://localhost:8081/subjects/{first_subject}/versions/latest",
                    shell=True,
                    capture_output=True
                )
                if schema_result[0]:
                    try:
                        schema_data = json.loads(schema_result[0])
                        schema_str = json.dumps(schema_data, indent=2)
                        # Print first 30 lines
                        print("\n".join(schema_str.split("\n")[:30]))
                    except json.JSONDecodeError:
                        print(schema_result[0])
        except json.JSONDecodeError:
            print(result[0])
    else:
        print("No schemas registered yet (will appear after first CDC event)")
    print()
    
    # Step 15: Show Kafka message count
    print_step(15, "Checking Kafka message counts")
    topics = ["pg.public.data1", "pg.public.data2", "pg.public.data3"]
    for topic in topics:
        result = run_command(
            f'docker exec kafka kafka-run-class kafka.tools.GetOffsetShell '
            f'--broker-list localhost:9092 --topic "{topic}"',
            shell=True,
            capture_output=True
        )
        if result[0]:
            # Parse offset output and sum
            count = 0
            for line in result[0].strip().split("\n"):
                if ":" in line:
                    parts = line.split(":")
                    if len(parts) >= 3:
                        try:
                            count += int(parts[2])
                        except ValueError:
                            pass
            print(f"  {topic}: {count} messages")
        else:
            print(f"  {topic}: 0 messages")
    print()
    
    # Step 16: Test AVRO consumer (briefly)
    print_step(16, "Testing AVRO consumer (5 seconds)")
    print("Running: python3 src/task3/avro_consumer.py")
    print("-" * 50)
    try:
        # Use signal-based timeout for cross-platform compatibility
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError("Consumer test timed out")
        
        # Set alarm for 5 seconds (Unix only)
        if hasattr(signal, 'SIGALRM'):
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(5)
            try:
                result = run_command(
                    ["python3", "src/task3/avro_consumer.py"],
                    capture_output=True
                )
                signal.alarm(0)  # Cancel alarm
                if result[0]:
                    print(result[0])
                if result[1]:
                    print(result[1], file=sys.stderr)
            except TimeoutError:
                print("Consumer test timed out after 5 seconds")
                signal.alarm(0)
        else:
            # Windows/fallback: just run without timeout
            result = run_command(
                ["python3", "src/task3/avro_consumer.py"],
                capture_output=True
            )
            if result[0]:
                print(result[0])
            if result[1]:
                print(result[1], file=sys.stderr)
    except Exception as e:
        print(f"Consumer test completed: {e}")
    print("-" * 50)
    print()
    
    # Step 17: Check Spark status
    print_step(17, "Spark cluster status")
    result = run_command("docker ps", shell=True, capture_output=True)
    if "spark" in result[0] and "Up" in result[0]:
        print("Spark master: Running")
        print("Spark UI: http://localhost:8080")
        print()
        print("Spark master logs (last 10 lines):")
        print(get_docker_logs("spark", 10))
        print()
        if "spark-worker" in result[0] and "Up" in result[0]:
            print("Spark worker: Running")
            print("Spark worker logs (last 10 lines):")
            print(get_docker_logs("spark-worker", 10))
    else:
        print("Spark containers not running")
    print()
    
    # Step 18: Show current state summary
    print_step(18, "Pipeline State Summary")
    print("-" * 50)
    print("PostgreSQL:")
    run_command(
        'docker exec pg_task1 psql -U pguser -d business_db -c '
        '"SELECT COUNT(*) as row_count, \'data1\' as table_name FROM data1 '
        'UNION ALL SELECT COUNT(*), \'data2\' FROM data2 '
        'UNION ALL SELECT COUNT(*), \'data3\' FROM data3;"',
        shell=True
    )
    print()
    print("Kafka Topics:")
    result = run_command(
        "docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list",
        shell=True,
        capture_output=True
    )
    if result[0]:
        for line in result[0].strip().split("\n"):
            if line.startswith("pg."):
                print(f"  - {line}")
    print()
    print("Debezium Connector:")
    result = run_command(
        "curl -s http://localhost:8083/connectors/postgres-connector/status",
        shell=True,
        capture_output=True
    )
    if result[0]:
        try:
            status = json.loads(result[0])
            connector_state = status.get("connector", {}).get("state", "unknown")
            print(f"  Status: {connector_state}")
        except json.JSONDecodeError:
            print("  Status: unknown")
    print()
    print("Schema Registry:")
    result = run_command(
        "curl -s http://localhost:8081/subjects",
        shell=True,
        capture_output=True
    )
    if result[0]:
        try:
            schemas = json.loads(result[0])
            print(f"  Registered schemas: {len(schemas)}")
        except json.JSONDecodeError:
            print("  Registered schemas: 0")
    print()
    
    # Step 19: Instructions for next steps
    print_step(19, "Next Steps")
    print("-" * 50)
    print("1. Generate CDC events by inserting data:")
    print('   docker exec -it pg_task1 psql -U pguser -d business_db -c "INSERT INTO data1 (key, value) VALUES (\'test\', \'value\');"')
    print()
    print("2. Run Spark job to process stream:")
    print("   ./scripts/run_spark_job.sh")
    print()
    print("3. Monitor Spark UI:")
    print("   http://localhost:8080")
    print()
    print("4. View real-time logs:")
    print("   docker logs -f <container-name>")
    print()
    
    print("=" * 50)
    print("Pipeline execution completed!")
    print("=" * 50)


if __name__ == "__main__":
    main()

