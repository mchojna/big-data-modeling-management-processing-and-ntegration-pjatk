from confluent_kafka import Producer
import time

p = Producer(
    {
        "bootstrap.servers": "localhost:9092",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 10,
        "batch.size": 65536,
    }
)

for i in range(10000):
    p.produce("events", key=str(i % 8), value=f"msg-{i}".encode())
    if i % 1000 == 0:
        p.flush()

p.flush()
print("done")
