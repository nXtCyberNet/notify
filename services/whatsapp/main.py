import asyncio
import asyncpg
import json
import time
import os
from confluent_kafka import Consumer
from twilio.rest import Client
# Add Prometheus client import
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Initialize Prometheus metrics
MESSAGES_RECEIVED = Counter('whatsapp_messages_received_total', 
                          'Total number of WhatsApp notification messages received')
WHATSAPP_SENT = Counter('whatsapp_sent_total', 
                      'Total number of WhatsApp messages sent', 
                      ['status'])  # status: success, failure
WHATSAPP_PROCESSING_TIME = Histogram('whatsapp_processing_seconds', 
                                   'Time spent processing WhatsApp notifications')
DB_ERRORS = Counter('whatsapp_db_errors_total', 
                  'Total number of database errors')
MISSING_PHONE = Counter('whatsapp_missing_phone_total', 
                      'Count of missing phone numbers')
DB_QUERY_TIME = Histogram('whatsapp_db_query_seconds',
                        'Time spent on database queries')
KAFKA_CONSUMER_LAG = Gauge('whatsapp_kafka_consumer_lag',
                         'Kafka consumer lag in messages')

# Twilio Credentials from environment variables
ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')
FROM_WHATSAPP = os.environ.get('TWILIO_FROM_WHATSAPP', 'whatsapp:+14155238886')

# Initialize Twilio client
twilio_client = Client(ACCOUNT_SID, AUTH_TOKEN)

# Database configuration from environment variables
PG_HOST = os.environ.get('PG_HOST')
PG_DB = os.environ.get('PG_DB')
PG_USER = os.environ.get('PG_USER')
PG_PASSWORD = os.environ.get('PG_PASSWORD')
PG_PORT = int(os.environ.get('PG_PORT', '5432'))

# Kafka configuration from environment variables
KAFKA_CONFIG = {
    'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'group.id': os.environ.get('KAFKA_GROUP_ID', 'mygroup'),
    'auto.offset.reset': os.environ.get('KAFKA_AUTO_OFFSET_RESET', 'earliest'),
}

# Kafka topic from environment variable
NOTIFICATION_TOPIC = os.environ.get('KAFKA_NOTIFICATION_TOPIC', 'notification-topic')

# Prometheus port from environment variable
PROMETHEUS_PORT = int(os.environ.get('PROMETHEUS_PORT', '8002'))

async def pg_con(): 
    try:
        return await asyncpg.connect(
            host=PG_HOST,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            port=PG_PORT
        )
    except Exception as e:
        DB_ERRORS.inc()
        print(f"❌ Database connection error: {e}")
        raise

async def get_user_phone(conn, user_id):
    start_time = time.time()
    try:
        row = await conn.fetchrow("SELECT phone_no FROM info WHERE orderid = $1", user_id)
        if row:
            return row['phone_no']
        MISSING_PHONE.inc()
        raise ValueError("Phone number not found.")
    except ValueError:
        # Re-raise the ValueError for specific handling
        raise
    except Exception as e:
        DB_ERRORS.inc()
        print(f"❌ Database query error: {e}")
        raise
    finally:
        DB_QUERY_TIME.observe(time.time() - start_time)

def send(to_number, body):
    # Get country code from environment variable with default to India (+91)
    country_code = os.environ.get('WHATSAPP_COUNTRY_CODE', '91')
    try:
        msg = twilio_client.messages.create(
            body=body,
            from_=FROM_WHATSAPP,
            to=f'whatsapp:+{country_code}{to_number}'
        )
        WHATSAPP_SENT.labels(status="success").inc()
        print(f"✅ Sent WhatsApp to {to_number}: SID {msg.sid}")
    except Exception as e:
        WHATSAPP_SENT.labels(status="failure").inc()
        print(f"❌ Failed to send WhatsApp to {to_number}: {e}")

async def kafka_consumer():
    conn = await pg_con()
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([NOTIFICATION_TOPIC])
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
            
            # Record message received
            MESSAGES_RECEIVED.inc()
            
            # Use histogram to track processing time
            start_time = time.time()
            
            try:
                mssg = msg.value().decode('utf-8')
                parts = mssg.split(',')
                order_id = int(parts[0])
                types = parts[1].strip()
                content = parts[2].strip()
                if types != 'prom':
                    phone = await get_user_phone(conn, order_id)
                    send(phone, content)
            except ValueError as e:
                # Specific handling for phone number not found
                print(f"⚠️ {e}")
            except Exception as e:
                print(f"⚠️ Processing error: {e}")
            finally:
                # Record the time taken to process the message
                WHATSAPP_PROCESSING_TIME.observe(time.time() - start_time)
    finally:
        consumer.close()
        await conn.close() 
        
async def main():
    # Start Prometheus HTTP server
    start_http_server(PROMETHEUS_PORT)
    print(f"📊 Prometheus metrics available at http://localhost:{PROMETHEUS_PORT}")
    await kafka_consumer()

if __name__ == '__main__':
    asyncio.run(main())
