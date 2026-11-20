import os
import json
import time
import random
import sys
from redis import Redis, ConnectionError, TimeoutError

# these are environment variables defined in the deployment file for the Redis connection
# so that we can modify Redis configurations from yaml file without touching the python code! 
REDIS_HOST = os.getenv("REDIS_HOST", "redis-service.smart-factory.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
QUEUE_NAME = os.getenv("QUEUE_NAME", "sensor_queue")

MACHINE_IDS = ["press-01", "cnc-02", "conveyor-03", "turbine-04"]

def connect_redis():
    retries = 0
    while True:
        try:
            r = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            print(f"[Producer] Connected to Redis at {REDIS_HOST}:{REDIS_PORT} !!", flush=True)
            return r
        except (ConnectionError, TimeoutError) as e:
            # here we upply the exponential backoff logic so this code doesn`t waste resources and 
            # network bandwedth commiting thousands of requests to Redis whose pod might be restarting!
            wait_time = min(2 ** retries, 30)
            print(f"[Producer] Redis connection failed: {e}, retrying in {wait_time}s...", flush=True)
            time.sleep(wait_time)
            retries += 1

def generate_reading():
    return {
        "machine_id": random.choice(MACHINE_IDS),
        "temperature": round(random.uniform(60.0, 105.5), 2),
        "vibration": round(random.uniform(0.1, 4.5), 2),
        "pressure": round(random.uniform(30.0, 90.0), 2),
    }

def main():
    r = connect_redis()
    
    while True:
        try:
            reading = generate_reading()
            # we need to turn python in-memory dictionary format into a human-readable format for
            # transmit and store in Redis queue , that is json
            payload = json.dumps(reading)
            r.rpush(QUEUE_NAME, payload)
            print(f"[Producer] Sent: {payload}", flush=True)
            time.sleep(2)     
        except (ConnectionError, TimeoutError) as e:
            print(f"[Producer] Redis error during send: {e}. Reconnecting...", flush=True)
            r = connect_redis()
        except Exception as e:
            print(f"[Producer] Unexpected error: {e}", flush=True)
            time.sleep(1)

if __name__ == "__main__":
    main()
