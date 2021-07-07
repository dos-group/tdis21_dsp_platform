from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    # TODO: Provide your server IP / hostname here
    bootstrap_servers='localhost:32090',
);

cells = [
    {"amount": 10, "average": 20},
    {"amount": 3,  "average": 345},
    {"amount": 10, "average": 472},
    {"amount": 6,  "average": 900},
    {"amount": 50, "average": 436},
    {"amount": 1,  "average": 354},
]

actions = [
    [],
    [],
    [],
    [{"cell": 2, "amount": 1, "spike": 50}],
    [{"cell": 2, "amount": 1, "spike": 60}],
    [{"cell": 2, "amount": 1, "spike": 70}],
    [],
    [],
    [{"cell": 3, "amount": 6, "spike": 80}], # 8
    [{"cell": 3, "amount": 6, "spike": 90}],
    [{"cell": 3, "amount": 6, "spike": 100}],
    [],
    [],
    [],
    [{"cell": 4, "amount": 4, "spike": 90}], # 14
    [{"cell": 4, "amount": 4, "spike": 80}], # 15
    [{"cell": 4, "amount": 5, "spike": 100}],
    [],
    [],
    [],
]

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

x=0
sensors_in_cells = []

for cell in cells:
    sensors_in_cells.append([{"id": i + x, "value": cell["average"], "unspiked": cell["average"]} for i in range(cell["amount"])])
    x += cell["amount"]

print(sensors_in_cells);

for (epoch, events) in enumerate(actions):
    print("\n\nCycle {}\n".format(epoch))

    for (i, cell) in enumerate(sensors_in_cells):
        print("Cell {}".format(i))
        modify = 0
        modify_value = 0
        for event in events:
            if event["cell"] == i:
                modify = event["amount"]
                modify_value = event["spike"]

        for (j, sensor) in enumerate(cell):
            sensor["unspiked"] += random.randrange(4) - 2
            if j < modify:
                sensor["value"] = sensor["unspiked"] + modify_value
            else:
                sensor["value"] = sensor["unspiked"]

            pkg = template_packet.copy()
            pkg["n"] = sensor["id"]
            pkg["t"] = int(time.time())
            pkg["v"] = sensor["value"]
            pkg["r"] = i + 1

            print("Sensor {}: {}".format(sensor["id"], pkg))
            future = producer.send('wgs-events-rich', json.dumps(pkg).encode('utf-8'))
            future.get(timeout=10)

    future = producer.send('wgs-alarms', json.dumps({"epoch": epoch}));
    future.get(timeout=10)

    time.sleep(30)


