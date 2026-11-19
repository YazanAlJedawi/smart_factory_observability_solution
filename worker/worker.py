
import os
import json
import time
import math
import socket
import ssl
from redis import Redis, ConnectionError, TimeoutError

# same env variables implemented in the producer.py which aim at having a unified configurable way
# to connect to Redis for both applications
REDIS_HOST = os.getenv("REDIS_HOST","redis-service.smart-factory.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
QUEUE_NAME = os.getenv("QUEUE_NAME", "sensor_queue")


# implementing sensory thersholds as env variables gives us more flexibility to reconfigure them as 
# surcumstances change instead of hardcoding them in code 
TEMPERATURE_WARN = float(os.getenv("TEMPERATURE_WARN", "95"))
TEMPERATURE_CRIT = float(os.getenv("TEMPERATURE_CRIT", "100"))

VIBRATION_WARN = float(os.getenv("VIBRATION_WARN", "3.0"))
VIBRATION_CRIT = float(os.getenv("VIBRATION_CRIT", "4.0"))

PRESSURE_WARN_LOW = float(os.getenv("PRESSURE_WARN_LOW", "40"))
PRESSURE_WARN_HIGH = float(os.getenv("PRESSURE_WARN_HIGH", "80"))

PRESSURE_CRIT_LOW = float(os.getenv("PRESSURE_CRIT_LOW", "35"))
PRESSURE_CRIT_HIGH = float(os.getenv("PRESSURE_CRIT_HIGH", "85"))

# logical catches to prevent an incorrectly configured threshold hierarchy.
if TEMPERATURE_WARN >= TEMPERATURE_CRIT:
    raise ValueError("Temperature warning must be below critical")

if VIBRATION_WARN >= VIBRATION_CRIT:
    raise ValueError("Vibration warning must be below critical")

if not (PRESSURE_CRIT_LOW < PRESSURE_WARN_LOW < PRESSURE_WARN_HIGH < PRESSURE_CRIT_HIGH):
    raise ValueError("Invalid pressure threshold hierarchy")


# checkmk configuration , how does the worker know where to send results ?!

CHECKMK_ENABLED = (os.getenv("CHECKMK_ENABLED", "true").lower() == "true")

CHECKMK_HOST = os.getenv("CHECKMK_HOST", "192.168.0.252")
CHECKMK_PORT = int(os.getenv("CHECKMK_PORT", "6557"))

CHECKMK_TLS_SERVER_NAME = os.getenv("CHECKMK_TLS_SERVER_NAME", "cmk")

CHECKMK_CA_FILE = os.getenv("CHECKMK_CA_FILE", "/etc/checkmk/ca.crt")

CHECKMK_SERVICE = os.getenv("CHECKMK_SERVICE_DESCRIPTION", "Customcheck")

def connect_redis():
    retries = 0
    while True:
        try:
            r = Redis(host=REDIS_HOST,port=REDIS_PORT,decode_responses=True)
            r.ping()
            print(f"[Worker] Connected to Redis at {REDIS_HOST}:{REDIS_PORT}",flush=True)
            return r
        except (ConnectionError, TimeoutError) as e:
            wait_time = min(2 ** retries, 30)
            print(
                f"[Worker] Redis connection failed: {e}, retrying in {wait_time}s...",flush=True)
            time.sleep(wait_time)
            retries += 1


# constructing a passive Checkmk result:
def push_to_checkmk(machine_id,temperature,vibration,pressure,state,state_text):

    if not CHECKMK_ENABLED:
        return

    target_host = machine_id

    # reject characters that could break Livestatus
    # command field separation.
    for field_name, field_value in (("host", target_host),("service", CHECKMK_SERVICE),):
 
        if any(c in str(field_value) for c in (";", "\n", "\r")):
            raise ValueError(f"Invalid character in Checkmk {field_name}")

    status_output = (
        f"{state_text} - Machine {machine_id}: "
        f"Temperature={temperature:.2f} C, "
        f"Vibration={vibration:.2f}, "
        f"Pressure={pressure:.2f}"
    )

    performance_data = (
        f"temperature={temperature:.2f};"
        f"{TEMPERATURE_WARN};{TEMPERATURE_CRIT};0;110 "

        f"vibration={vibration:.2f};"
        f"{VIBRATION_WARN};{VIBRATION_CRIT};0;5 "

        f"pressure={pressure:.2f};"
        f"{PRESSURE_WARN_HIGH}:{PRESSURE_KRIT_HIGH};30;90 "
        f"pressure={pressure:.2f};"
        f"{PRESSURE_WARN_LOW}:{PRESSURE_CRIT_LOW};30;90"
    )

    command = (
        f"COMMAND [{int(time.time())}] "
        f"PROCESS_SERVICE_CHECK_RESULT;"
        f"{target_host};"
        f"{CHECKMK_SERVICE};"
        f"{state};"
        f"{status_output} | {performance_data}\n"
    )

    # validate the Checkmk server's TLS certificate.
    context = ssl.create_default_context(cafile=CHECKMK_CA_FILE)

    # open an encrypted Livestatus connection.
    with socket.create_connection((CHECKMK_HOST, CHECKMK_PORT),timeout=5) as raw_socket:
        with context.wrap_socket(raw_socket,server_hostname=CHECKMK_TLS_SERVER_NAME) as tls_socket:
            tls_socket.sendall(command.encode("utf-8"))
            tls_socket.shutdown(socket.SHUT_WR)
    print(f"[Worker] Sent {state_text} result to Checkmk: "f"{target_host}/{CHECKMK_SERVICE}",flush=True)


# here the sensor telemetry would be ingested and matched against the preset thersholds
# so a final intry is formulated accordingly and pushed to the checkmk monitoring stack

def process_data(data):

    if not isinstance(data, dict):
        raise ValueError("Sensor payload must be a JSON object")

    machine_id = str(data.get("machine_id", "")).strip()

    if not machine_id:
        raise ValueError("Sensor payload has no machine_id")

    values = (temperature, vibration, pressure)
    if not all(math.isfinite(value) for value in values):
        raise ValueError("Sensor payload contains invalid numeric values")

    temperature = float(data.get("temperature", 0.0))
    vibration = float(data.get("vibration", 0.0))
    pressure = float(data.get("pressure", 0.0))

    values = (temperature, vibration, pressure)


    critical_reasons = []
    warning_reasons = []

    # .2f means the float value would be formated so there is 2 digits
    # after the decimal point ,EX: 42.285 -> 42.28 / for ease of reading!
    if temperature >= TEMPERATURE_CRIT:
        critical_reasons.append(f"Temperature critical: {temperature:.2f} C")

    elif temperature >= TEMPERATURE_WARN:
        warning_reasons.append(f"Temperature elevated: {temperature:.2f} C")


    if vibration >= VIBRATION_CRIT:
        critical_reasons.append(f"Vibration critical: {vibration:.2f}")

    elif vibration >= VIBRATION_WARN:
        warning_reasons.append(f"Vibration elevated: {vibration:.2f}")

    if pressure < PRESSURE_CRIT_LOW:
        critical_reasons.append(f"Pressure critically low: {pressure:.2f}")

    elif pressure > PRESSURE_CRIT_HIGH:
        critical_reasons.append(f"Pressure critically high: {pressure:.2f}")

    elif pressure < PRESSURE_WARN_LOW:
        warning_reasons.append(f"Pressure low: {pressure:.2f}")

    elif pressure > PRESSURE_WARN_HIGH:
        warning_reasons.append(f"Pressure high: {pressure:.2f}")

    # overall state

    if critical_reasons:
        state = 2
        state_text = "CRITICAL | ".join(critical_reasons + warning_reasons)

    elif warning_reasons:
        state = 1
        state_text = "WARNING | ".join(warning_reasons)

    else:

        state = 0
        state_text = "OK"

    print(
        f"[Worker] {state_text} | Machine={machine_id} | "
        f"Temperature={temperature:.2f} C | "
        f"Vibration={vibration:.2f} | "
        f"Pressure={pressure:.2f}",
        flush=True
    )

    push_to_checkmk(machine_id,temperature,vibration,pressure,state,state_text)



def main():
    r = connect_redis()
    print(f"[Worker] ready and listening on queue: {QUEUE_NAME}",flush=True)
    while True:
        try:
            item = r.blpop(QUEUE_NAME, timeout=5)
            if not item:
                continue

            # blpop returns queue_name and payload
            queue_name, payload = item

            try:
                data = json.loads(payload)
                process_data(data)

            except (json.JSONDecodeError, ValueError, TypeError) as e:

                print(f"[Worker] Invalid sensor message discarded: {e}",flush=True)

            except Exception as e:

                print(f"[Worker] Checkmk submission failed: {e}",flush=True)

                # put failed telemetry back at the front
                # of the Redis list for a later retry.
                try:
                    r.lpush(queue_name, payload)
                    print("[Worker] Message returned to Redis queue",flush=True)

                except (ConnectionError, TimeoutError) as redis_error:

                    print(f"[Worker] Could not requeue message: {redis_error}",flush=True)

                    r = connect_redis()

                time.sleep(3)

        except (ConnectionError, TimeoutError) as e:

            print(f"[Worker] Redis error: {e}. Reconnecting...",flush=True)

            r = connect_redis()

        except Exception as e:

            print(f"[Worker] Unexpected error: {e}",flush=True)
            time.sleep(1)

if __name__ == "__main__":
    main()

