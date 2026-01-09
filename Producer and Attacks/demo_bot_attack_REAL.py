# demo_bot_attack_REAL.py

"""
Module: demo_bot_attack_REAL
Description:
    This script simulates a malicious "Bot Attack" on the ad-tech pipeline.
    Unlike the standard traffic generator, this script is designed to flood
    the system with high-frequency "click" events from a single IP address.

    Purpose:
    - To test the "Bot Detection" logic in the real-time dashboard.
    - To verify that the Hits-Per-User ratio spikes correctly.
    - To trigger system alerts (High/Critical Risk).

    Key Mechanics:
    - Uses a single, static IP (192.168.1.666) to ensure a high HPU ratio.
    - Sends data to the same Kafka topic ('ad_stream') as legitimate traffic.
    - Allows configurable duration and intensity (rate) via CLI.

Dependencies:
    - kafka-python: To produce attack events to the broker.
"""

# Import KafkaProducer to send messages to the Kafka broker
from kafka import KafkaProducer
# Import datetime classes to generate timestamps for the events
from datetime import datetime, timedelta, timezone
# Import json for serializing the event dictionary into a string
import json
# Import time for delays (sleep) and performance tracking
import time
# Import argparse to handle command-line arguments (rate, duration, campaign)
import argparse
# Import sys for system-level operations like exit codes
import sys
# Import uuid to generate unique IDs for every fake event
import uuid

# ----------------------------------------------------------------------
# Configuration Constants
# ----------------------------------------------------------------------
# The address of the Kafka Broker to attack
KAFKA_BROKER = "localhost:9092"
# The target topic (must match the one used by the dashboard/Spark)
KAFKA_TOPIC = "ad_stream"

# Simulated bot farm configuration
# We use a single, static IP to intentionally skew the "Hits Per User" metric.
# 192.168.1.666 is used as a blatantly obvious fake IP for debugging clarity.
BOT_IP = "192.168.1.666"


def test_kafka_connection():
    """
    Verifies that the Kafka broker is reachable before starting the attack.

    This prevents the script from crashing mid-loop if the broker is down.

    Returns:
        bool: True if connection is successful, False otherwise.
    """
    print("🔍 Testing Kafka connection...")
    try:
        # Attempt to create a producer with a short timeout
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000,  # Give up after 5 seconds
            api_version_auto_timeout_ms=5000,  # Fail fast on version check
        )
        # If successful, close it immediately
        producer.close()
        print("✅ Kafka connection successful!")
        return True
    except Exception as e:
        # Log failure and provide troubleshooting steps
        print(f"❌ Kafka connection failed: {e}")
        print(f"\n⚠️  Troubleshooting:")
        print(f"   1. Check if Kafka is running: netstat -an | grep 9092")
        print(f"   2. Check if Zookeeper is running (required for Kafka)")
        print(f"   3. Verify Kafka broker address: {KAFKA_BROKER}")
        return False


def generate_bot_event(campaign_id="campaign_1"):
    """
    Constructs a single malicious ad event designed to look like bot traffic.

    Characteristics of a Bot Event:
    - Fixed IP (BOT_IP).
    - Fixed Action ("click" - bots want to drain ad budgets).
    - Invalid Country Code ("XX") to test data quality filters.
    - Fixed Device Type.

    Args:
        campaign_id (str): The ID of the campaign to attack.

    Returns:
        dict: The constructed event payload.
    """
    event = {
        # Timestamp generation:
        # Use UTC+2 (Gaza/Hebron time) to match the main traffic generator's timezone.
        # This ensures the dashboard displays the attack *now* rather than 2 hours ago.
        "timestamp": datetime.now(timezone(timedelta(hours=2))).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],

        # Generate a unique ID for the event itself (even bots generate unique requests)
        "event_id": str(uuid.uuid4()),

        # KEY BOT INDICATOR: Reusing the exact same IP address repeatedly
        "ip_address": BOT_IP,

        # Use an invalid country code to potentially trigger data quality warnings
        "country": "XX",

        # Bots usually "click" to exhaust the advertiser's budget
        "action": "click",

        # The specific campaign being targeted
        "campaign_id": campaign_id,

        # Static device type (simplistic bot behavior)
        "device": "mobile",
    }
    return event


def run_bot_attack(duration_seconds=60, rate_per_second=100, campaign_id="campaign_1"):
    """
    Main Attack Logic: Floods the Kafka topic with bot events.

    Args:
        duration_seconds (int): Total duration of the attack in seconds.
        rate_per_second (int): Number of fake clicks to send per second.
        campaign_id (str): The target campaign identifier.
    """

    # Step 1: Pre-flight check
    if not test_kafka_connection():
        print("\n❌ Cannot proceed without Kafka connection.")
        sys.exit(1)

    # Step 2: Print Attack Parameters (Banner)
    print("\n" + "=" * 70)
    print("🚨 BOT ATTACK SIMULATOR 🚨")
    print("=" * 70)
    print(f"Target: {campaign_id}")
    print(f"Rate: {rate_per_second} clicks/second")
    print(f"Duration: {duration_seconds} seconds")
    print(f"Bot IP: {BOT_IP}")
    print(f"Total fake clicks: {duration_seconds * rate_per_second:,}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print("=" * 70)

    # Step 3: Initialize the Kafka Producer with high-throughput settings
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type="gzip",  # Use GZIP to reduce network load
            acks=1,  # Wait for leader ack (faster than 'all')
            retries=3,  # Retry a few times on failure
            max_in_flight_requests_per_connection=5,  # Allow pipelining
            request_timeout_ms=10000,  # 10s timeout
        )
        print("✅ Kafka producer initialized")
    except Exception as e:
        print(f"❌ Failed to create Kafka producer: {e}")
        sys.exit(1)

    # Countdown to give user a chance to prepare the dashboard
    print("\n⏳ Starting attack in 3 seconds...")
    print("   (Watch your dashboard for the spike!)")
    time.sleep(3)

    # Initialize counters and timers
    start_time = time.time()
    event_count = 0
    error_count = 0

    try:
        print("\n🚀 Attack started!\n")

        # Main Loop: Run until duration expires
        while (time.time() - start_time) < duration_seconds:
            batch_start = time.time()

            # Inner Loop: Send the required number of events for this second
            for _ in range(rate_per_second):
                try:
                    # Generate the payload
                    event = generate_bot_event(campaign_id)
                    # Send asynchronously (fire and forget for speed)
                    future = producer.send(KAFKA_TOPIC, value=event)
                    event_count += 1
                except Exception as e:
                    error_count += 1
                    # Only print the first error to avoid flooding the console
                    if error_count == 1:
                        print(f"⚠️  Error sending event: {e}")

            # Force push the batch to the broker to ensure real-time appearance
            producer.flush()

            # Progress Reporting: Every 10 batches (approx 10 seconds)
            if event_count % (rate_per_second * 10) == 0:
                elapsed = time.time() - start_time
                actual_rate = event_count / elapsed
                print(f"⚡ Sent {event_count:,} bot clicks | "
                      f"Elapsed: {elapsed:.1f}s | "
                      f"Rate: {actual_rate:.1f}/s | "
                      f"Errors: {error_count}")

            # Rate Limiting: Sleep for the remainder of the second
            # This ensures we don't dump everything instantly and finish too early
            batch_duration = time.time() - batch_start
            sleep_time = max(0, 1.0 - batch_duration)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        # Handle manual stop (Ctrl+C)
        print("\n\n⚠️  Attack interrupted by user")
    except Exception as e:
        # Handle unexpected crashes
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup: Ensure all messages are sent and connection closed
        print("\n🔄 Flushing remaining messages...")
        producer.flush()
        producer.close()

        # Final Statistics Calculation
        elapsed = time.time() - start_time
        actual_rate = event_count / elapsed if elapsed > 0 else 0

        # Print Summary Report
        print("\n" + "=" * 70)
        print("✅ ATTACK COMPLETE")
        print("=" * 70)
        print(f"Total fake clicks sent: {event_count:,}")
        print(f"Duration: {elapsed:.1f} seconds")
        print(f"Actual rate: {actual_rate:.1f} events/sec")
        print(f"Errors: {error_count}")
        print("\n📊 Check your dashboard in ~10-15 seconds for:")
        print("   - Spike in Total Hits")
        print("   - Unchanged Unique Users (same IP)")
        print("   - Massive increase in Hits/User ratio")
        print("   - Bot Probability jumping to HIGH/CRITICAL")
        print("   - New alerts in Alerts panel")

        if error_count > 0:
            print(f"\n⚠️  WARNING: {error_count} events failed to send")
            print("   Check Kafka broker logs for details")

        print("=" * 70)


# Standard boilerplate to run the script
if __name__ == "__main__":
    # Setup command-line argument parsing
    parser = argparse.ArgumentParser(
        description="Simulate a bot attack by flooding Kafka with fake clicks"
    )
    # Argument: Duration of the attack
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Attack duration in seconds (default: 60)"
    )
    # Argument: Intensity (Clicks per second)
    parser.add_argument(
        "--rate",
        type=int,
        default=100,
        help="Bot clicks per second (default: 100)"
    )
    # Argument: Target Campaign ID
    parser.add_argument(
        "--campaign",
        type=str,
        default="campaign_1",
        help="Target campaign ID (default: campaign_1)"
    )
    # Argument: Connection Test Only
    parser.add_argument(
        "--test-only",
        action="store_true",
        help="Only test Kafka connection and exit"
    )

    args = parser.parse_args()

    # If --test-only flag is set, run check and exit
    if args.test_only:
        test_kafka_connection()
        sys.exit(0)

    # Warn the user before generating fake data
    print("\n⚠️  WARNING: This will inject fake traffic into your pipeline!")
    print("Press Ctrl+C within 2 seconds to cancel...\n")
    time.sleep(2)

    # Execute the attack
    run_bot_attack(
        duration_seconds=args.duration,
        rate_per_second=args.rate,
        campaign_id=args.campaign
    )