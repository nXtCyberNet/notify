import asyncio
import asyncpg
import json
from confluent_kafka import Consumer
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, messaging
import time
import os
# Remove dotenv import
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Initialize Prometheus metrics
MESSAGES_RECEIVED = Counter('notification_messages_received_total', 
                          'Total number of notification messages received')
NOTIFICATIONS_SENT = Counter('notification_sent_total', 
                           'Total number of notifications sent successfully', 
                           ['status'])  # status: success, failure
NOTIFICATION_PROCESSING_TIME = Histogram('notification_processing_seconds', 
                                      'Time spent processing notifications')
DB_ERRORS = Counter('notification_db_errors_total', 
                  'Total number of database errors')
MISSING_TOKENS = Counter('notification_missing_tokens_total', 
                       'Count of missing FCM tokens')
DB_QUERY_TIME = Histogram('notification_db_query_seconds',
                        'Time spent on database queries')
KAFKA_CONSUMER_LAG = Gauge('notification_kafka_consumer_lag',
                         'Kafka consumer lag in messages')

# Initialize Firebase Admin SDK
# Get path from environment variable
firebase_service_account_path = os.environ.get("FIREBASE_SERVICE_ACCOUNT_PATH")
firebase_creds = credentials.Certificate(firebase_service_account_path)
firebase_admin.initialize_app(firebase_creds)

# Database configuration from environment variables
PG_HOST = os.environ.get("PG_HOST")
PG_DB = os.environ.get("PG_DB")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))

# Kafka configuration from environment variables
KAFKA_CONFIG = {
    'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    'group.id': os.environ.get("KAFKA_GROUP_ID", "mygroup"),
    'auto.offset.reset': os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest"),
}

# Kafka topic from environment variable
NOTIFICATION_TOPIC = os.environ.get("KAFKA_NOTIFICATION_TOPIC", "notification-topic")

# Prometheus port from environment variable
PROMETHEUS_PORT = int(os.environ.get("PROMETHEUS_PORT", "8123"))

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

async def get_fcm_token(conn, user_id):
    start_time = time.time()
    try:
        row = await conn.fetchval("SELECT fmc_token FROM info WHERE orderid = $1", int(user_id))
        if not row:
            MISSING_TOKENS.inc()
        return row
    except Exception as e:
        DB_ERRORS.inc()
        print(f"❌ Database query error: {e}")
        return None
    finally:
        DB_QUERY_TIME.observe(time.time() - start_time)

async def push(fcm_token, title, message):
    loop = asyncio.get_running_loop()
    
    # Create message
    msg = messaging.Message(
        notification=messaging.Notification(
            title=title,
            body=message,
        ),
        token=fcm_token,
    )
    
    try:
        # Send message (wrapped in run_in_executor because messaging.send is blocking)
        response = await loop.run_in_executor(None, messaging.send, msg)
        NOTIFICATIONS_SENT.labels(status="success").inc()
        print(f"📲 Push notification sent to {fcm_token}, message_id: {response}")
    except Exception as e:
        NOTIFICATIONS_SENT.labels(status="failure").inc()
        print(f"❌ Failed to send push: {e}")

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
                data = msg.value().decode('utf-8').split(',')
                user_id = data[0]
                title = data[1]
                message = data[2]

                # Push notification
                fcm_token = await get_fcm_token(conn, user_id)
                if fcm_token:
                    await push(fcm_token, title, message)
                else:
                    print(f"⚠️ No FCM token found for user {user_id}")
            except Exception as e:
                print(f"⚠️ Processing error: {e}")
            finally:
                # Record the time taken to process the message
                NOTIFICATION_PROCESSING_TIME.observe(time.time() - start_time)
    finally:
        consumer.close()
        await conn.close()

# Entry point
async def main():
    # Start Prometheus HTTP server
    start_http_server(PROMETHEUS_PORT)
    print(f"📊 Prometheus metrics available at http://localhost:{PROMETHEUS_PORT}")
    await kafka_consumer()

if __name__ == '__main__':
    asyncio.run(main())
