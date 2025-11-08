from confluent_kafka import Consumer

c = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "lab-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)

c.subscribe(["events"])

count = 0
try:
    while True:
        msg = c.poll(1.0)
        if not msg:
            continue
        if msg.error():
            print("Error: ", msg.error())
        # process
        count += 1
        if count % 1000 == 0:
            c.commit(asynchronous=False)
            print("Processed: ", count)
finally:
    c.close()
