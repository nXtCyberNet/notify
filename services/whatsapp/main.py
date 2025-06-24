import asyncio
import asyncpg
import json
from confluent_kafka import Consumer
from twilio.rest import Client
import os

# Twilio Credentials
ACCOUNT_SID = os.getenv('ACCOUNT_SID')
AUTH_TOKEN = os.getenv('AUTH_TOKEN')
FROM_WHATSAPP = 'whatsapp:+14155238886'

twilio_client = Client(ACCOUNT_SID, AUTH_TOKEN)

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": os.getenv('PG_PASSWORD'),  # Use environment variable for password
    "host": os.getenv("HOST"),
    "port": 5432
}
PG_HOST = DB_CONFIG["host"]
PG_DB = DB_CONFIG["dbname"]
PG_USER = DB_CONFIG["user"]
PG_PASSWORD = DB_CONFIG["password"]


conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest',
}

async def pg_con(): 
    return await asyncpg.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

async def get_user_phone(conn, user_id):
    row = await conn.fetchrow("SELECT phone_no FROM info WHERE orderid = $1", user_id)
    if row:
        return row['phone_no']
    raise ValueError("Phone number not found.")

def send(to_number, body):
    try:
        msg = twilio_client.messages.create(
            body=body,
            from_=FROM_WHATSAPP,
            to=f'whatsapp:+91{to_number}'
        )
        print(f"✅ Sent WhatsApp to {to_number}: SID {msg.sid}")
    except Exception as e:
        print(f"❌ Failed to send WhatsApp to {to_number}: {e}")

async def kafka_consumer():
    conn = await pg_con()
    consumer = Consumer(conf)
    consumer.subscribe(['notification-topic'])
    loop = asyncio.get_running_loop()
    try:
        while True:
            msg = await loop.run_in_executor(None, consumer.poll, 1.0)
            if msg is None:
                await asyncio.sleep(0.1)
                continue
            if msg.error():
                print("❌ Kafka error:", msg.error())
                continue
            try:
                mssg = msg.value().decode('utf-8')
                parts = mssg.split(',')
                order_id = int(parts[0])
                types = parts[1].strip()
                content = parts[2].strip()
                if types != 'prom':
                    phone = await get_user_phone(conn, order_id)
                    send(phone, content)
            except Exception as e:
                print(f"⚠️ Processing error: {e}")
    finally:
        consumer.close()
        await conn.close() 
        
async def main():
    await kafka_consumer()

if __name__ == '__main__':
    asyncio.run(main())                   

    
