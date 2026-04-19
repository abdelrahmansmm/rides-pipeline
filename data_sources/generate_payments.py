from pymongo import MongoClient, ASCENDING
import random
import uuid
from datetime import datetime, timedelta

client = MongoClient(
    host='localhost',
    port=27017
)

db = client['payments']
col = db['payment_records']

col.drop()
print("Dropped existing payment_records collection")

col.create_index([('trip_id', ASCENDING)], unique=True)
col.create_index([('created_at', ASCENDING)])
col.create_index([('provider', ASCENDING)])

PROVIDERS = ['fawry', 'vodafone_cash', 'instapay', 'mastercard', 'visa', 'cash']
PROMO_CODES = ['FIRSTRIDE', 'RAMADAN25', 'SUMMER10', None, None, None]

documents = []
for trip_id in range(1, 50_001): # The 50k trips in PostgreSQL
    provider = random.choice(PROVIDERS)
    amount = round(random.uniform(15.0, 500.0), 2)
    promo = random.choice(PROMO_CODES)
    discount = round(amount * random.uniform(0.1, 0.25), 2) if promo else 0

    doc = {
        'trip_id': trip_id,
        'transaction_ref': str(uuid.uuid4()),
        'provider': provider,
        'amount_egp': amount,
        'discount_egp': discount,
        'net_amount_egp': round(amount - discount, 2),
        'promo_code': promo,
        'status': random.choice(['success','success','success','refunded','failed']),
        'created_at': datetime.now() - timedelta(days=random.randint(0,90), hours=random.randint(0,23)),
        'provider_metadata': {
            'response_code': random.choice(['00','01','99']),
            'masked_card': f'****{random.randint(1000,9999)}' if provider in ['mastercard','visa'] else None
        }
    }
    documents.append(doc)

    # Batch insert every 1000 documents
    if len(documents) == 1000:
        col.insert_many(documents)
        documents = []
        print(f"Inserted up to trip_id {trip_id}...")

if documents:
    col.insert_many(documents)

print(f"[+] MongoDB: {col.count_documents({})} payment records inserted")
client.close()
