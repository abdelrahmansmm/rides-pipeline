import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
    acks='all',
    retries=3,
    linger_ms=5,
)

CAIRO_LAT = (29.85, 30.15)
CAIRO_LONG = (31.10, 31.55)

def random_cairo_coords():
    return (
        round(random.uniform(*CAIRO_LAT), 6),
        round(random.uniform(*CAIRO_LONG), 6)
    )

ACTIVE_DRIVERS = list(range(1, 201))
trip_counter = 50_001

print('Kafka Produce is running ...')
while True:
    for driver_id in random.sample(ACTIVE_DRIVERS, k=50):
        lat, long = random_cairo_coords()

        gps_event = {
            'event_type': 'gps_ping',
            'driver_id': driver_id,
            'latitude': lat,
            'longitude': long,
            'speed_kmh': round(random.uniform(0, 80), 1),
            'heading': random.randint(0, 359),
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }

        # Key by driver_id ensures all events from same driver -> same partition
        producer.send('driver.gps', key=driver_id, value=gps_event)

        if random.random() < 0.03:
            trip_event = {
                'event_type': 'trip_completed',
                'trip_id': trip_counter,
                'driver_id': driver_id,
                'fare_egp': round(random.uniform(20, 400), 2),
                'distance_km': round(random.uniform(1, 30), 2),
                'zone_id': random.randint(1, 8),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
            producer.send('trip_events', key=trip_counter, value=trip_event)
            trip_counter += 1

    producer.flush()
    time.sleep(5)