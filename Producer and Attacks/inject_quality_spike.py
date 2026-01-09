# inject_quality_spike.py

"""
Module: inject_quality_spike
Description:
    This script injects a controlled burst of "malformed" or "corrupted" data
    into the Kafka pipeline to test the Data Quality monitoring features of the
    dashboard.

    By intentionally setting fields like 'ip_address' to None, we can verify
    that the Spark Structured Streaming job correctly flags these events as
    malformed and that the Streamlit dashboard displays the "Degraded" or
    "Unhealthy" status.

    Key Features:
    - High Error Rate Injection: Defaults to 50% corruption to make the spike obvious.
    - Gaza Time Synchronization: Ensures timestamps match the dashboard's timezone.
    - Bulk Injection: Sends 1,000 events rapidly to create a noticeable spike.

Dependencies:
    - kafka-python: For producing messages to the Kafka broker.
"""

# Import the KafkaProducer to allow publishing messages to a Kafka broker
from kafka import KafkaProducer
# Import json for serializing dictionary objects into JSON strings
import json
# Import uuid for generating unique event identifiers
import uuid
# Import random for probability logic (determining if an event should be corrupted)
import random
# Import time for sleep functions (though mostly used for buffer management here)
import time
# Import datetime classes for timestamp generation and timezone handling
from datetime import datetime, timedelta, timezone

# ----------------------------------------------------------------------
# 1. Configuration Constants
# ----------------------------------------------------------------------
# The address of the Kafka Broker (Server:Port)
KAFKA_BROKER = "localhost:9092"
# The specific Kafka topic name where events will be published
KAFKA_TOPIC = "ad_stream"
# The total number of events to send in this specific test burst
TOTAL_EVENTS = 1000
# The probability (0.0 to 1.0) that a generated event will be corrupted
# 0.5 = 50% error rate (This is high to ensure the dashboard alert triggers)
ERROR_RATE = 0.5

# ----------------------------------------------------------------------
# 2. Gaza Timezone Definition
# ----------------------------------------------------------------------
# We define a fixed timezone offset (UTC+2) to match the main traffic generator.
# This ensures that the injected dirty data appears at the "current" time
# on the dashboard, rather than 2 hours in the past or future.
GAZA_TZ = timezone(timedelta(hours=2))


def get_gaza_time():
    """
    Returns the current time in Gaza (UTC+2), formatted for the pipeline.

    The format "%Y-%m-%d %H:%M:%S.%f" is compatible with Spark's
    standard timestamp parsers. We trim the last 3 digits of the microseconds
    ([:-3]) to simulate millisecond precision.

    Returns:
        str: The formatted timestamp string.
    """
    # Get current time in the specific timezone and format it
    return datetime.now(GAZA_TZ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def run_injection():
    """
    Main execution function to inject corrupted data.

    Logic:
        1. Connects to the Kafka Broker.
        2. Iterates `TOTAL_EVENTS` times.
        3. Randomly decides whether to corrupt the event based on `ERROR_RATE`.
        4. Sends the event to the `ad_stream` topic.
        5. Flushes the producer periodically to manage network buffers.
        6. Prints a summary of the injection test.
    """
    # Notify user that connection is attempting
    print(f"🔌 Connecting to Kafka at {KAFKA_BROKER}...")

    try:
        # Initialize the Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],  # Connection string
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # Auto-serialize to JSON bytes
            linger_ms=10  # Slight delay to batch messages for better throughput
        )
    except Exception as e:
        # Handle connection errors (e.g., Broker down)
        print(f"❌ Connection failed: {e}")
        return

    # Print start banner with test parameters
    print(f"🚨 Injecting {TOTAL_EVENTS} events with {ERROR_RATE * 100}% corruption rate...")
    print(f"🕒 Timestamp used: {get_gaza_time()} (Gaza Time)")

    # Counter to track how many "bad" events we actually sent
    bad_count = 0

    # Loop to generate and send the specific number of events
    for i in range(TOTAL_EVENTS):
        # Determine if this specific event should be corrupted
        is_bad = random.random() < ERROR_RATE

        # Construct the base event structure (valid by default)
        event = {
            "timestamp": get_gaza_time(),  # Use synchronized time
            "event_id": str(uuid.uuid4()),  # Unique ID
            "country": "US",  # Default country
            "action": "click",  # Default action
            "campaign_id": "test_campaign",  # Test campaign ID
            "device": "mobile"  # Default device
        }

        if is_bad:
            # === SIMULATE DATA QUALITY ISSUE ===
            # We explicitly set 'ip_address' to None (NULL).
            # The Spark pipeline is configured to check for NULL IPs or Campaigns.
            # This will increment the 'malformed_events' counter in the database.
            event["ip_address"] = None
            bad_count += 1
        else:
            # Valid event: Assign a proper IP address
            event["ip_address"] = "192.168.1.100"

        # Send the event to the Kafka topic
        producer.send(KAFKA_TOPIC, value=event)

        # Periodically flush the producer (every 100 events)
        # This prevents the local buffer from filling up if the loop is too fast
        # and ensures messages are actually sent over the network.
        if i % 100 == 0:
            producer.flush()

    # Final flush to ensure no messages remain in the local buffer
    producer.flush()
    # Close the connection cleanly
    producer.close()

    # Print the final summary report
    print("\n✅ Injection Complete!")
    print(f"   - Total Events: {TOTAL_EVENTS}")
    print(f"   - Malformed (Bad): {bad_count}")
    print(f"   - Valid (Good): {TOTAL_EVENTS - bad_count}")
    print("\nCheck your Dashboard 'Data Quality' section in ~10 seconds.")


# Standard boilerplate to run the injection if executed directly
if __name__ == "__main__":
    run_injection()