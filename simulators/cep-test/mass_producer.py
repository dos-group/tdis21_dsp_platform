from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    # TODO: Provide your server IP / hostname here
    bootstrap_servers='130.149.249.40:9092',
    # TODO: Authentication here
);


template_packet = {
    "n": 1,
    "t": int(time.time()),
    "v": 0.985,
    "g": "UNKNOWN",
    "r": 1,
    "ty": "SAMPLE",
    "u": "Sp",
    "c": 0.0,
};

start = time.time()
i = 0
while start + 1 > time.time():
    pkg = template_packet.copy()
    pkg["n"] = random.randrange(0, 1000)
    pkg["v"] = random.randrange(0, 1000)
    pkg["t"] = int(start)
    pkg["r"] = random.randrange(0, 100)
    i += 1

    producer.send('wgs-events-rich', json.dumps(pkg).encode('utf-8'))

print(i)
