import psycopg2
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('ar-EG')

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    dbname='postgres_database',
    user='postgres_user',
    password='postgres_pass'
)
cur = conn.cursor()

# Zones
ZONES = [
    ('Maadi', 'Cairo', 29.9602, 31.2569),
    ('Zamalek', 'Cairo', 30.0626, 31.2197),
    ('Heliopolis', 'Cairo', 30.0911, 31.3264),
    ('New Cairo', 'Cairo', 30.0271, 31.4842),
    ('Downtown', 'Cairo', 30.0444, 31.2358),
    ('Nasr City', 'Cairo', 30.0626, 31.3419),
    ('Dokki', 'Giza', 30.0385, 31.2118),
    ('6th October', 'Giza', 29.9290, 30.9327),
]

for name, gov, lat, long in ZONES:
    cur.execute('''
                INSERT INTO zones (zone_name, governorate, latitude, longitude)
                VALUES (%s, %s, %s, %s)
                ''',
        (name, gov, lat, long)
    )

conn.commit()
print(f"[+] Inserted {len(ZONES)} zones.")

# Drivers
VEHICLE_TYPES = ['economy', 'business', 'motorbike']
for i in range(500):
    nid = ''.join([str(random.randint(0, 9)) for _ in range(14)])
    cur.execute('''
                INSERT INTO drivers (full_name, national_id, phone, vehicle_type, vehicle_plate, rating, registered_zone)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ''',
        (
            fake.name(),
            nid,
            f"+201{random.randint(0, 9)}{random.randint(10000000, 99999999)}",
            random.choice(VEHICLE_TYPES),
            f"{random.randint(1, 999)}{chr(random.randint(65, 90))}{chr(random.randint(65, 90))}",
            round(random.uniform(3.5, 5.0), 2),
            random.randint(1, 8)
        )
    )

conn.commit()
print("[+] Inserted 500 drivers.")

# Users
for i in range(2000):
    cur.execute('''
                INSERT INTO users (full_name, phone, email)
                VALUES (%s, %s, %s)
                ''',
        (
            fake.name(),
            f"+201{random.randint(0,9)}{random.randint(10000000,99999999)}",
            fake.email()
        )
    )

conn.commit()
print("[+] Inserted 2000 users.")

# Historical 50,000 trips (last 90 days)
STATUS_WEIGHTS = ['completed']*70 + ['cancelled']*15 + ['in_progress']*15
PAYMENT_METHODS = ['cash','wallet','credit_card']
CANCEL_REASONS  = ['driver_cancelled','user_cancelled','no_driver_found', None]

for i in range(50_000):
    status   = random.choice(STATUS_WEIGHTS)
    pickup_zone = random.randint(1, len(ZONES))
    dropoff_zone = random.randint(1, len(ZONES))
    vehicle_type    = random.choice(VEHICLE_TYPES)
    payment = random.choice(PAYMENT_METHODS)
    requested_at = datetime.now() - timedelta(days=random.randint(0,90),
                                          hours=random.randint(0,23),
                                          minutes=random.randint(0,59))
    distance = round(random.uniform(0.5, 35.0), 2)
    base_fare = round(distance * random.uniform(8, 14), 2)
    surge = round(base_fare * random.uniform(1.0, 2.5), 2)
    duration = int(distance * random.uniform(2.5, 6.0))

    cur.execute('''
                INSERT INTO trips
                (user_id, driver_id, pickup_zone_id, dropoff_zone_id, status, vehicle_type, payment_method, requested_at,
                base_fare, surge_fare, distance_km, duration_min, cancellation_reason)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ''',
        (
            random.randint(1,2000),
            random.randint(1,500),
            pickup_zone,
            dropoff_zone,
            status,
            vehicle_type,
            payment,
            requested_at,
            base_fare,
            surge,
            distance,
            duration,
            random.choice(CANCEL_REASONS) if status=='cancelled' else None
        )
    )

conn.commit()
cur.close(); conn.close()
print('[+] Inserted 50,000 trips — seed complete!')
