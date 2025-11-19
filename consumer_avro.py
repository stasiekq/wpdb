from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.avro import AvroDeserializer

def main():
    schema_registry_conf = {"url": "http://localhost:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    consumer_conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "avro-test-group",
        "auto.offset.reset": "earliest"
    }

    consumer = Consumer(consumer_conf)

    topic = "wpdb.public.data1"
    consumer.subscribe([topic])

    print("Consumer AVRO start...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            continue

        # Pobranie schematu z registry
        subject = f"{topic}-value"
        schema = schema_registry_client.get_latest_version(subject)
        avro_deserializer = AvroDeserializer(
            schema_registry_client,
            schema.schema.schema_str
        )

        value = avro_deserializer(
            msg.value(),
            SerializationContext(topic, MessageField.VALUE)
        )

        print("AVRO MESSAGE:", value)

if __name__ == "__main__":
    main()
