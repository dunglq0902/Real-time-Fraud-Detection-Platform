"""
=============================================================
Real-Time Transaction Data Simulator
=============================================================
"""

import json
import os
import random
import time
import uuid
import math
import logging
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("DataSimulator")

# ── Configuration ──────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TX_TOPIC = os.getenv("TRANSACTIONS_TOPIC", "transactions")
RULES_TOPIC = os.getenv("RULES_TOPIC", "fraud-rules")
EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "50"))
FRAUD_RATIO = float(os.getenv("FRAUD_RATIO", "0.03"))

# ── Realistic Data Pools ──────────────────────────────────
ACCOUNTS = [f"ACC-{str(i).zfill(6)}" for i in range(1, 501)]
MERCHANTS = [f"MERCH-{str(i).zfill(4)}" for i in range(1, 201)]
CARDS = {acc: f"CARD-{str(i).zfill(8)}" for i, acc in enumerate(ACCOUNTS, start=1)}
CHANNELS = ["ATM", "ONLINE", "POS", "MOBILE"]
EVENT_TYPES = ["LOGIN", "TRANSFER", "PAYMENT", "WITHDRAWAL", "CHANGE_PASSWORD"]

# Major cities with lat/lng for geo simulation
LOCATIONS = [
    {"city": "Ho Chi Minh", "lat": 10.8231, "lng": 106.6297},
    {"city": "Hanoi", "lat": 21.0285, "lng": 105.8542},
    {"city": "Da Nang", "lat": 16.0544, "lng": 108.2022},
    {"city": "Singapore", "lat": 1.3521, "lng": 103.8198},
    {"city": "Tokyo", "lat": 35.6762, "lng": 139.6503},
    {"city": "New York", "lat": 40.7128, "lng": -74.0060},
    {"city": "London", "lat": 51.5074, "lng": -0.1278},
    {"city": "Sydney", "lat": -33.8688, "lng": 151.2093},
    {"city": "Dubai", "lat": 25.2048, "lng": 55.2708},
    {"city": "Seoul", "lat": 37.5665, "lng": 126.9780},
]

# Track per-account state for realistic sequences
account_state: dict = {}


def get_account_state(account_id: str) -> dict:
    """Get or create account state for generating realistic sequences."""
    if account_id not in account_state:
        home_loc = random.choice(LOCATIONS[:3])  # Vietnamese cities as home
        account_state[account_id] = {
            "home_location": home_loc,
            "last_location": home_loc,
            "last_tx_time": 0,
            "avg_amount": max(1.0, random.gauss(500, 200)),
        }
    return account_state[account_id]


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance in km between two lat/lng points."""
    R = 6371  # Earth's radius in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _make_event(account_id: str, ts: int, amount: float, event_type: str,
                lat: float, lng: float, merchant_id: str, is_fraud: int) -> dict:
    """Helper to construct a full event dict with all required schema fields."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "card_id": CARDS.get(account_id, f"CARD-{account_id[-6:]}"),
        "timestamp": ts,
        "produced_at": int(time.time() * 1000),
        "amount": round(amount, 2),
        "event_type": event_type,
        "channel": random.choice(CHANNELS),
        "location": json.dumps({"latitude": round(lat, 6), "longitude": round(lng, 6)}),
        "status": "PENDING",
        "merchant_id": merchant_id,
        "is_fraud": is_fraud,
    }


def generate_normal_transaction(account_id: str) -> dict:
    """Generate a legitimate-looking transaction."""
    state = get_account_state(account_id)
    loc = state["home_location"]

    # Add small jitter to location (within same city)
    lat = loc["lat"] + random.gauss(0, 0.01)
    lng = loc["lng"] + random.gauss(0, 0.01)

    # Normal amounts follow log-normal distribution
    amount = max(1.0, random.lognormvariate(math.log(state["avg_amount"]), 0.5))

    event_type = random.choices(
        ["TRANSFER", "PAYMENT", "WITHDRAWAL", "LOGIN"],
        weights=[0.3, 0.4, 0.2, 0.1],
        k=1
    )[0]

    state["last_location"] = {"lat": lat, "lng": lng}
    state["last_tx_time"] = int(time.time() * 1000)

    return _make_event(account_id, int(time.time() * 1000), amount,
                       event_type, lat, lng, random.choice(MERCHANTS), is_fraud=0)


def generate_brute_force_sequence(account_id: str) -> list:
    """
    Generate Account Takeover pattern:
    3-5 LOGIN_FAILED → LOGIN_SUCCESS → high-value TRANSFER
    """
    state = get_account_state(account_id)
    loc = state["home_location"]
    events = []
    base_time = int(time.time() * 1000)

    # 3-5 failed logins (not fraud themselves, but part of the pattern)
    num_failures = random.randint(3, 5)
    total_events = num_failures + 2  # failed + success + transfer
    for i in range(num_failures):
        ts = base_time - (total_events - 1 - i) * 500
        events.append(_make_event(
            account_id, ts, 0.0, "LOGIN_FAILED",
            loc["lat"] + random.gauss(0, 0.5),
            loc["lng"] + random.gauss(0, 0.5),
            "", is_fraud=0
        ))

    # Successful login
    login_time = base_time - 500
    events.append(_make_event(
        account_id, login_time, 0.0, "LOGIN_SUCCESS",
        loc["lat"] + random.gauss(0, 0.5),
        loc["lng"] + random.gauss(0, 0.5),
        "", is_fraud=0
    ))

    # High-value transfer immediately after — THIS is the fraud event
    events.append(_make_event(
        account_id, base_time, random.uniform(5000, 50000), "TRANSFER",
        loc["lat"] + random.gauss(0, 0.5),
        loc["lng"] + random.gauss(0, 0.5),
        random.choice(MERCHANTS), is_fraud=1
    ))

    return events


def generate_rapid_transactions(account_id: str) -> list:
    """
    Generate Velocity Attack pattern:
    3-6 rapid TRANSFER/PAYMENT within 30 seconds (smurfing).
    """
    state = get_account_state(account_id)
    loc = state["home_location"]
    events = []
    base_time = int(time.time() * 1000)

    num_tx = random.randint(3, 6)
    for i in range(num_tx):
        ts = base_time - (num_tx - 1 - i) * 500
        events.append(_make_event(
            account_id, ts, random.uniform(100, 2000),
            random.choice(["TRANSFER", "PAYMENT"]),
            loc["lat"] + random.gauss(0, 0.005),
            loc["lng"] + random.gauss(0, 0.005),
            random.choice(MERCHANTS), is_fraud=1
        ))

    return events


def generate_impossible_travel(account_id: str) -> list:
    """
    Generate Impossible Travel pattern:
    Two transactions from locations > 150km apart within < 15 minutes.
    """
    state = get_account_state(account_id)
    events = []
    base_time = int(time.time() * 1000)

    # First transaction at home location
    loc1 = state["home_location"]
    events.append(_make_event(
        account_id, base_time - 2000, random.uniform(50, 1000), "PAYMENT",
        loc1["lat"], loc1["lng"],
        random.choice(MERCHANTS), is_fraud=0
    ))

    # Second transaction from a far-away location within minutes
    far_locations = [l for l in LOCATIONS
                     if haversine_distance(loc1["lat"], loc1["lng"], l["lat"], l["lng"]) > 150]
    if far_locations:
        loc2 = random.choice(far_locations)
    else:
        loc2 = {"lat": loc1["lat"] + 10, "lng": loc1["lng"] + 10}

    events.append(_make_event(
        account_id, base_time, random.uniform(200, 5000),
        random.choice(["PAYMENT", "WITHDRAWAL"]),
        loc2["lat"], loc2["lng"],
        random.choice(MERCHANTS), is_fraud=1
    ))

    return events


def generate_high_amount_transaction(account_id: str) -> dict:
    """Generate a single suspiciously high-value transaction."""
    state = get_account_state(account_id)
    loc = state["home_location"]
    # Amount far exceeding the normal range (Q3 + 1.5*IQR)
    amount = random.uniform(10000, 100000)

    return _make_event(
        account_id, int(time.time() * 1000), amount,
        random.choice(["TRANSFER", "PAYMENT", "WITHDRAWAL"]),
        loc["lat"] + random.gauss(0, 0.01),
        loc["lng"] + random.gauss(0, 0.01),
        random.choice(MERCHANTS), is_fraud=1
    )


def generate_micro_transactions(account_id: str) -> list:
    """
    Generate Micro Transactions (Carding/Smurfing) pattern:
    5-8 financial transactions under $10 within 2 minutes.
    Matches CEP pattern: 5+ txns with amount < $10 within 2 min.
    """
    state = get_account_state(account_id)
    loc = state["home_location"]
    events = []
    base_time = int(time.time() * 1000)

    num_tx = random.randint(5, 8)  # Must be >= 5 to trigger CEP pattern
    for i in range(num_tx):
        ts = base_time - (num_tx - 1 - i) * 500
        events.append(_make_event(
            account_id, ts, round(random.uniform(0.50, 9.99), 2),  # Under $10
            random.choice(["TRANSFER", "PAYMENT", "WITHDRAWAL"]),
            loc["lat"] + random.gauss(0, 0.005),
            loc["lng"] + random.gauss(0, 0.005),
            random.choice(MERCHANTS), is_fraud=1
        ))

    return events



def delivery_report(err, msg):
    """Kafka delivery callback."""
    if err is not None:
        logger.error(f"Delivery failed: {err}")


def push_initial_rules(producer: Producer):
    """Push initial fraud detection rules to the rules topic."""
    initial_rules = [
        {
            "rule_id": "RULE_001",
            "rule_name": "High Amount Threshold",
            "field": "amount",
            "operator": "GREATER_THAN",
            "threshold": 10000.0,
            "severity": "HIGH",
            "version": 1,
            "active": True,
        },
        {
            "rule_id": "RULE_002",
            "rule_name": "Blacklisted Merchant",
            "field": "merchant_id",
            "operator": "IN",
            "threshold": ["MERCH-0199", "MERCH-0198", "MERCH-0197"],
            "severity": "CRITICAL",
            "version": 1,
            "active": True,
        },
        {
            "rule_id": "RULE_003",
            "rule_name": "Nighttime Large Transfer",
            "field": "amount",
            "operator": "GREATER_THAN",
            "threshold": 5000.0,
            "time_window": {"start_hour": 0, "end_hour": 5},
            "severity": "MEDIUM",
            "version": 1,
            "active": True,
        },
    ]

    for rule in initial_rules:
        producer.produce(
            RULES_TOPIC,
            key=rule["rule_id"],
            value=json.dumps(rule).encode("utf-8"),
            callback=delivery_report,
        )
    producer.flush()
    logger.info(f"Pushed {len(initial_rules)} initial rules to '{RULES_TOPIC}'")


def main():
    """Main simulator loop."""
    logger.info("=" * 60)
    logger.info("  FRAUD DETECTION DATA SIMULATOR")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP}")
    logger.info(f"  Topic: {TX_TOPIC}")
    logger.info(f"  Rate:  {EVENTS_PER_SECOND} events/sec")
    logger.info(f"  Fraud: {FRAUD_RATIO * 100:.1f}%")
    logger.info("=" * 60)

    # Wait for Kafka to be ready
    producer = None
    connected = False
    for attempt in range(30):
        try:
            p = Producer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "compression.type": "snappy",
                "batch.size": 65536,
                "linger.ms": 10,
                "acks": "all",
                "queue.buffering.max.messages": 100000,
            })
            # Test connectivity
            p.list_topics(timeout=5)
            producer = p
            connected = True
            logger.info("Connected to Kafka successfully!")
            break
        except Exception as e:
            logger.warning(f"Kafka not ready (attempt {attempt + 1}/30): {e}")
            time.sleep(5)

    if not connected or producer is None:
        logger.error("Failed to connect to Kafka after 30 attempts. Exiting.")
        return

    # Push initial rules
    push_initial_rules(producer)

    # Main event loop
    total_sent = 0
    fraud_sent = 0
    interval = 1.0 / EVENTS_PER_SECOND
    fraud_event_buffer: list = []

    logger.info("Starting continuous transaction generation...")

    while True:
        try:
            # If we have buffered fraud events (multi-event sequences), send them first
            if fraud_event_buffer:
                event = fraud_event_buffer.pop(0)
                producer.produce(
                    TX_TOPIC,
                    key=event["account_id"],    # The same AccountID place in the same Partition
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )
                total_sent += 1
                if event.get("is_fraud", 0) == 1:
                    fraud_sent += 1
            else:
                # Decide: normal vs fraud
                if random.random() < FRAUD_RATIO:
                    account = random.choice(ACCOUNTS)
                    fraud_type = random.choices(
                        ["brute_force", "rapid_tx", "impossible_travel", "high_amount", "micro_tx"],
                        weights=[0.25, 0.20, 0.25, 0.10, 0.20],
                        k=1
                    )[0]

                    if fraud_type == "brute_force":
                        fraud_event_buffer.extend(generate_brute_force_sequence(account))
                    elif fraud_type == "rapid_tx":
                        fraud_event_buffer.extend(generate_rapid_transactions(account))
                    elif fraud_type == "impossible_travel":
                        fraud_event_buffer.extend(generate_impossible_travel(account))
                    elif fraud_type == "micro_tx":
                        fraud_event_buffer.extend(generate_micro_transactions(account))
                    else:
                        event = generate_high_amount_transaction(account)
                        producer.produce(
                            TX_TOPIC,
                            key=event["account_id"],
                            value=json.dumps(event).encode("utf-8"),
                            callback=delivery_report,
                        )
                        total_sent += 1
                        fraud_sent += 1
                else:
                    # Normal transaction
                    account = random.choice(ACCOUNTS)
                    event = generate_normal_transaction(account)
                    producer.produce(
                        TX_TOPIC,
                        key=event["account_id"],
                        value=json.dumps(event).encode("utf-8"),
                        callback=delivery_report,
                    )
                    total_sent += 1

            # Periodic stats logging
            if total_sent % 500 == 0 and total_sent > 0:
                fraud_pct = (fraud_sent / total_sent * 100) if total_sent > 0 else 0
                logger.info(
                    f"Stats: total={total_sent}, fraud={fraud_sent} "
                    f"({fraud_pct:.1f}%), buffer={len(fraud_event_buffer)}"
                )

            producer.poll(0)
            time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("Shutting down simulator...")
            break
        except Exception as e:
            logger.error(f"Error generating event: {e}", exc_info=True)
            time.sleep(1)

    producer.flush()
    logger.info(f"Simulator stopped. Total events: {total_sent}, Fraud: {fraud_sent}")


if __name__ == "__main__":
    main()
