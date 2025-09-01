import json
import time
import psycopg2
import redis
import os
import threading
from fastapi import FastAPI 

app = FastAPI()

DB_URL = os.environ.get('DATABASE_URL')
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')

@app.get("/")
def health_check():
    return {"status": "Consumer is running."}


def connect_to_db():
    """Establishes a connection to the PostgreSQL database."""
    while True:
        try:
            conn = psycopg2.connect(DB_URL)
            print("Successfully connected to the database.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Could not connect to the database: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def create_table(conn):
    """Creates the raw_logs table if it doesn't already exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_logs (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL,
                status_code VARCHAR(3) NOT NULL,
                ip_address VARCHAR(15)
            );
        """)
        conn.commit()
        print("Table 'raw_logs' is ready.")

def insert_log(conn, log_data):
    """Inserts a single log event into the raw_logs table."""
    with conn.cursor() as cur:
        sql = "INSERT INTO raw_logs (timestamp, status_code, ip_address) VALUES (%s, %s, %s);"
        cur.execute(sql, (
            log_data.get("timestamp"),
            log_data.get("status_code"),
            log_data.get("ip")
        ))
        conn.commit()

def consume_and_process():
    """The main background task for consuming messages."""
    db_connection = connect_to_db()
    create_table(db_connection)
    
    r = redis.Redis.from_url(REDIS_URL)
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe("log-channel")
    print("Consumer thread started. Subscribed to 'log-channel' in Redis.")

    for message in pubsub.listen():
        log_data_str = message['data'].decode('utf-8')
        log_data = json.loads(log_data_str)
        insert_log(db_connection, log_data)
        print(f"Logged event with status code: {log_data.get('status_code')}")


consumer_thread = threading.Thread(target=consume_and_process, daemon=True)
consumer_thread.start()