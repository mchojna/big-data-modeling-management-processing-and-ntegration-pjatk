"""
Avro-based Kafka consumer using Confluent Schema Registry.
- Consumes from 'users.avro'.
- Deserializes Avro payloads into Python dicts.
Run:
python consumer_avro.py
Stop:
CTRL+C
"""

import sys
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

TOPIC = "users.avro"
BOOTSTRAP_SERVERS = "localhost:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


def dict_from_user(obj, ctx):
    # For this example, we simply return the dict from the deserializer
    return obj


def main():
    # 1) Schema Registry client
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # 2) Avro deserializer – reader schema is optional; we use writer schema
    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=None,  # use writer schema
        from_dict=dict_from_user,  # convert dict -> desired type (here: same dict)
    )

    # 3) Kafka consumer
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "users.avro.group",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC])

    print(f"Consuming Avro messages from topic '{TOPIC}'... (CTRL+C to stop)")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue

            # 4) Deserialize
            user = avro_deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE),
            )

            print(f"Received key={msg.key()} value={user}")

    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
