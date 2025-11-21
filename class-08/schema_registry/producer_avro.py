"""
Avro-based Kafka producer using Confluent Schema Registry.
- Serializes User records with Avro.
- Registers the schema in Schema Registry (if not registered yet).
- Sends messages to topic 'users.avro'.
Run:
python producer_avro.py
"""

import json
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)

TOPIC = "users.avro"

BOOTSTRAP_SERVERS = "localhost:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


def load_avro_schema_str(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def user_to_dict(user_obj, ctx):
    """
    Convert our Python dict to something AvroSerializer understands (dict).
    Here user_obj is already a dict, so just return it.
    """
    return user_obj


def main():
    # 1) Schema Registry client
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # 2) Load Avro schema from file
    user_schema_str = load_avro_schema_str("user.avsc")

    # 3) Avro serializer for VALUES
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=user_schema_str,
        to_dict=user_to_dict,
        # Optional config: auto.register.schemas=True/False (default True)
    )

    # 4) Configure Kafka producer with serializers
    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
    }
    producer = Producer(producer_conf)

    def delivery_report(err, msg):
        if err is not None:
            print(f"Delivery failed for record {msg.key()}: {err}")
        else:
            print(
                f"Record produced to {msg.topic()} partition {msg.partition()} offset {msg.offset()}"
            )

    print(f"Producing Avro messages to topic '{TOPIC}'...")

    try:
        for i in range(5):
            user_id = str(uuid.uuid4())

            ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            user = {
                "id": user_id,
                "email": f"user{i}@example.com",
                "full_name": f"User {i}",
                "signup_ts": ts_ms,
            }

            # We must explicitly call the serializer to get bytes
            value_bytes = avro_serializer(
                user, SerializationContext(TOPIC, MessageField.VALUE)
            )
            key_bytes = StringSerializer("utf_8")(
                user_id, SerializationContext(TOPIC, MessageField.KEY)
            )

            producer.produce(
                topic=TOPIC,
                key=key_bytes,
                value=value_bytes,
                on_delivery=delivery_report,
            )

            producer.poll(0)  # serve delivery callbacks
            time.sleep(0.5)
    finally:
        print("Flushing...")
        producer.flush()
        print("Done.")


if __name__ == "__main__":
    main()
