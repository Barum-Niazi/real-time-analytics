import json
import time
import uuid
import random
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "user_events_raw"

USERS = [f"user_{i}" for i in range(1, 51)]
PLATFORMS = ["web", "ios", "android"]
APP_VERSIONS = ["1.0.0", "1.1.0", "1.2.0"]

EVENT_TYPES = {
    "page_view": ["page_name"],
    "button_click": ["element_id", "feature_name"],
    "feature_used": ["feature_name"],
    "error_occurred": ["error_code", "error_message", "severity"]
}

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5
)

def utc_now():
    return datetime.now(timezone.utc)

def random_event_time():
    # 90% on-time, 10% late (up to 2 minutes)
    if random.random() < 0.9:
        return utc_now()
    return utc_now() - timedelta(seconds=random.randint(10, 120))

def build_event():
    user_id = random.choice(USERS)
    event_name = random.choice(list(EVENT_TYPES.keys()))
    required_props = EVENT_TYPES[event_name]

    event_properties = {}
    for prop in required_props:
        if prop == "page_name":
            event_properties[prop] = random.choice(["home", "search", "checkout"])
        elif prop == "element_id":
            event_properties[prop] = random.choice(["btn_pay", "btn_signup"])
        elif prop == "feature_name":
            event_properties[prop] = random.choice(["search", "recommendations"])
        elif prop == "error_code":
            event_properties[prop] = random.choice(["E_TIMEOUT", "E_500"])
        elif prop == "error_message":
            event_properties[prop] = "Something went wrong"
        elif prop == "severity":
            event_properties[prop] = random.choice(["low", "medium", "high"])

    event_time = random_event_time()

    return {
        "schema_version": "1.0",
        "event_id": str(uuid.uuid4()),
        "event_name": event_name,
        "event_time": event_time.isoformat(),
        "ingest_time": utc_now().isoformat(),
        "user_id": user_id,
        "anonymous_id": None,
        "session_id": None,
        "platform": random.choice(PLATFORMS),
        "device": {
            "device_id": str(uuid.uuid4()),
            "os": "ios" if random.random() < 0.5 else "android",
            "os_version": "latest",
            "device_type": "mobile"
        },
        "app": {
            "app_name": "demo_app",
            "app_version": random.choice(APP_VERSIONS),
            "environment": "prod"
        },
        "geo": {
            "country": "US",
            "region": "CA",
            "city": "San Francisco"
        },
        "event_properties": event_properties,
        "context": {
            "ip_address": "127.0.0.1",
            "user_agent": "event-generator"
        }
    }

def main():
    print("Starting event generator...")
    while True:
        event = build_event()
        producer.send(
            TOPIC,
            key=event["user_id"],
            value=event
        )
        print(f"Sent event {event['event_name']} for {event['user_id']}")
        time.sleep(random.uniform(0.2, 1.0))

if __name__ == "__main__":
    main()
