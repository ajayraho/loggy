import time
import datetime
import random
from faker import Faker
import redis
import json
import os

fake = Faker()

status_codes = ["200"]*10 + ["404"]*2 + ["403", "301"] + ["500"]*5 # Reduced 500s for normal operation

methods = ["GET", "POST", "PUT", "DELETE"]

urls = ["/home", "/products/123", "/cart", "/checkout", "/user/profile", "/api/data"]

def create_redis_client():
    """Creates a Redis client instance."""
    print("Connecting to Redis...")
    try:
        # For local testing, it connects to localhost. For cloud, it uses the REDIS_URL env var.
        redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379')
        client = redis.Redis.from_url(redis_url)
        client.ping() # Check the connection
        print("Successfully connected to Redis.")
        return client
    except Exception as e:
        print(f"Could not connect to Redis: {e}")
        return None

def generate_log_line():
    """Generates a single log line as a dictionary."""
    return {
        "ip": fake.ipv4(),
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "method": random.choice(methods),
        "url": random.choice(urls),
        "status_code": random.choice(status_codes),
        "user_agent": fake.user_agent()
    }

if __name__ == "__main__":
    redis_client = create_redis_client()
    
    if not redis_client:
        exit()
        
    print("Starting log generation... Press Ctrl+C to stop.")
    try:
        while True:
            log_data = generate_log_line()
            
            redis_client.publish('log-channel', json.dumps(log_data))
            
            print(f"Published: {log_data}")
            
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("\nLog generator stopped.")
    finally:
        if redis_client:
            redis_client.close()