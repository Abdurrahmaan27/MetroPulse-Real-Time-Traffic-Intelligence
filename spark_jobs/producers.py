import json
import time
import random
import datetime as dt
from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:29092"})

segments = [f"seg_{i:03d}" for i in range(40)]
stations = [f"st_{i:03d}" for i in range(15)]

def now():
    return dt.datetime.utcnow().isoformat()

while True:
    # Traffic message
    tmsg = {
        "event_time": now(),
        "segment_id": random.choice(segments),
        "speed_kmh": max(0, random.gauss(45, 12)),
        "congestion_pct": min(100, max(0, random.gauss(35, 20))),
        "source": "sim"
    }
    
    # Air-quality message
    amsg = {
        "event_time": now(),
        "station_id": random.choice(stations),
        "pm25": max(0, random.gauss(40, 15)),
        "pm10": max(0, random.gauss(70, 25)),
        "no2": max(0, random.gauss(28, 9)),
        "source": "sim"
    }

    p.produce("sensors.traffic.v1", json.dumps(tmsg).encode())
    p.produce("sensors.air.v1", json.dumps(amsg).encode())

    p.poll(0)
    p.flush()
    time.sleep(0.2)  
    print("sent traffic + air event")
