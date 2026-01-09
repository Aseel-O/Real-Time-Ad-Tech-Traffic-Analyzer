# pipeline_diagnostic.py

"""
Module: pipeline_diagnostic
Description:
    This is a comprehensive diagnostic utility for the Ad-Tech Real-Time Pipeline.
    It sequentially validates every component of the architecture to ensure the
    system is ready for data processing.

    Diagnostic Checks:
    1. Dependencies: Verifies that required Python libraries (kafka-python, pymongo) are installed.
    2. Kafka Connectivity: Tests connection to the broker and ability to produce messages.
    3. Topic Existence: Checks if the target topic ('ad_stream') exists.
    4. MongoDB Connectivity: Tests connection to the database and verifies collections exist.
    5. Spark Status: Infers if Spark is running by checking for recent writes in MongoDB.
    6. Generator Status: Checks if the traffic generator is active by listening for new messages.
    7. End-to-End Test: Sends a unique 'tracer' event to Kafka and waits to see if it
       appears in MongoDB, verifying the entire ETL pipeline.

    Usage:
        python pipeline_diagnostic.py
"""

# Import sys to handle system-specific parameters and functions (like exit codes)
import sys
# Import time to handle sleep delays (waiting for Spark processing)
import time
# Import datetime classes for timestamp generation and timezone handling
from datetime import datetime, timezone
# Import json for serializing test events and deserializing Kafka messages
import json

# Print the header for the diagnostic tool
print("=" * 70)
print("PIPELINE DIAGNOSTIC TOOL")
print("=" * 70)
print("\n1️⃣ Checking Python dependencies...")

# ----------------------------------------------------------------------
# 1. Dependency Check
# ----------------------------------------------------------------------
try:
    # Attempt to import Kafka components to ensure the library is installed
    from kafka import KafkaProducer, KafkaConsumer

    print("   ✅ kafka-python installed")
except ImportError:
    # Handle missing dependency gracefully with installation instructions
    print("   ❌ kafka-python NOT installed")
    print("      Fix: pip install kafka-python")
    sys.exit(1)  # Critical failure: cannot proceed without this library

try:
    # Attempt to import PyMongo to ensure database connectivity tools are present
    from pymongo import MongoClient

    print("   ✅ pymongo installed")
except ImportError:
    # Handle missing dependency
    print("   ❌ pymongo NOT installed")
    print("      Fix: pip install pymongo")
    sys.exit(1)  # Critical failure

# ----------------------------------------------------------------------
# Configuration Constants
# ----------------------------------------------------------------------
# The address of the Kafka Broker to test
KAFKA_BROKER = "localhost:9092"
# The topic name used by the pipeline
KAFKA_TOPIC = "ad_stream"

print("\n2️⃣ Testing Kafka connection...")
print(f"   Broker: {KAFKA_BROKER}")
print(f"   Topic: {KAFKA_TOPIC}")

# ----------------------------------------------------------------------
# 2. Kafka Connectivity Check
# ----------------------------------------------------------------------
try:
    # specific test: Attempt to initialize a Producer to check broker reachability
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # Auto-serialize JSON
        request_timeout_ms=5000,  # Fail fast if broker is down (5s)
    )
    print("   ✅ Kafka producer connected")

    # specific test: Send a dummy diagnostic event to verify write permissions
    test_event = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "event_id": "diagnostic_test",
        "ip_address": "127.0.0.1",
        "country": "US",
        "action": "click",
        "campaign_id": "diagnostic",
        "device": "desktop",
    }

    # Send asynchronously
    future = producer.send(KAFKA_TOPIC, value=test_event)
    # Block and wait for the broker to acknowledge receipt (verifies network path)
    future.get(timeout=5)
    producer.close()
    print("   ✅ Successfully sent test event to Kafka")

except Exception as e:
    # detailed troubleshooting info if connection fails
    print(f"   ❌ Kafka connection failed: {e}")
    print("\n   TROUBLESHOOTING:")
    print("   - Is Kafka running? Check with: jps (should see 'Kafka')")
    print("   - Is Zookeeper running? (Required for Kafka)")
    print("   - Start Kafka:")
    print("     Windows: bin\\windows\\kafka-server-start.bat config\\server.properties")
    print("     Linux/Mac: bin/kafka-server-start.sh config/server.properties")
    sys.exit(1)

print("\n3️⃣ Testing Kafka topic existence...")

# ----------------------------------------------------------------------
# 3. Topic Existence Check
# ----------------------------------------------------------------------
try:
    # Create a consumer to query metadata from the broker
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        consumer_timeout_ms=2000,
    )
    # Fetch list of all topics currently known to the broker
    topics = consumer.topics()

    # Check if our specific pipeline topic is in that list
    if KAFKA_TOPIC in topics:
        print(f"   ✅ Topic '{KAFKA_TOPIC}' exists")
    else:
        # Warning only: Topic might auto-create when the generator starts writing
        print(f"   ⚠️  Topic '{KAFKA_TOPIC}' not found")
        print(f"   Available topics: {topics}")
        print("\n   The topic will be auto-created when generator starts")

    consumer.close()

except Exception as e:
    print(f"   ⚠️  Could not list topics: {e}")

print("\n4️⃣ Testing MongoDB connection...")

# ----------------------------------------------------------------------
# 4. MongoDB Connectivity Check
# ----------------------------------------------------------------------
try:
    # Connect to local MongoDB instance
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
    # Force a network call to verify connection (lazy connection otherwise)
    client.server_info()
    print("   ✅ MongoDB connected")

    # Access the specific database
    db = client["adtech"]
    collections = db.list_collection_names()
    print(f"   ✅ Database 'adtech' exists")
    print(f"   Collections: {collections}")

    # Check if there is any data in the main metrics collection
    traffic_count = db["traffic_metrics"].count_documents({})
    print(f"   📊 traffic_metrics: {traffic_count} documents")

    # If data exists, show the timestamp of the most recent record
    if traffic_count > 0:
        latest = db["traffic_metrics"].find_one(sort=[("window_start", -1)])
        print(f"   📅 Latest data: {latest.get('window_start')}")

    client.close()

except Exception as e:
    # detailed troubleshooting info if Mongo fails
    print(f"   ❌ MongoDB connection failed: {e}")
    print("\n   TROUBLESHOOTING:")
    print("   - Is MongoDB running? Check with: mongosh")
    print("   - Start MongoDB:")
    print("     Windows: net start MongoDB")
    print("     Linux: sudo systemctl start mongod")
    print("     Mac: brew services start mongodb-community")
    sys.exit(1)

print("\n5️⃣ Checking if Spark Streaming is running...")

# ----------------------------------------------------------------------
# 5. Spark Streaming Status Check (Inferred)
# ----------------------------------------------------------------------
try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["adtech"]

    # Import timedelta locally for time math
    from datetime import timedelta

    # Define a "Recent" window (last 5 minutes)
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=5)

    # Count how many documents have been written in the last 5 minutes
    # Spark updates these aggregations every ~10-60 seconds.
    recent_traffic = db["traffic_metrics"].count_documents(
        {"window_start": {"$gte": cutoff}}
    )

    if recent_traffic > 0:
        # If we see fresh data, Spark is definitely running
        print(f"   ✅ Spark appears to be running (found {recent_traffic} recent windows)")
    else:
        # If no fresh data, Spark might be down or paused
        print("   ⚠️  No recent data in traffic_metrics")
        print("   Spark Streaming might not be running")
        print("\n   START SPARK:")
        print("   sbt run")
        print("   (or compile to JAR and use spark-submit)")

    client.close()

except Exception as e:
    print(f"   ⚠️  Could not check Spark status: {e}")

print("\n6️⃣ Checking if ad_traffic_generator.py is running...")

# ----------------------------------------------------------------------
# 6. Generator Status Check
# ----------------------------------------------------------------------
try:
    # Create a consumer that reads from the *latest* offset
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',  # Ignore old history, want new events
        consumer_timeout_ms=3000,  # Listen for exactly 3 seconds
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("   ⏳ Listening for events (3 seconds)...")

    event_count = 0
    # Loop over any messages received in the 3 second window
    for message in consumer:
        event_count += 1

    consumer.close()

    if event_count > 0:
        print(f"   ✅ Generator is running ({event_count} events received in 3s)")
    else:
        print("   ⚠️  No events detected in 3 seconds")
        print("   Generator might not be running")
        print("\n   START GENERATOR:")
        print("   python ad_traffic_generator.py")

except Exception as e:
    print(f"   ⚠️  Could not check generator: {e}")

print("\n7️⃣ Testing end-to-end data flow...")

# ----------------------------------------------------------------------
# 7. End-to-End Data Flow Test
# ----------------------------------------------------------------------
# This is the most critical test. It injects a known "tracer" event
# into Kafka and polls MongoDB to see if Spark successfully processed it.
try:
    # Generate a unique ID for this specific test run (using timestamp)
    test_id = f"e2e_test_{int(time.time())}"

    # Construct the tracer event
    test_event = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "event_id": test_id,
        "ip_address": "10.0.0.99",
        "country": "US",
        "action": "click",
        "campaign_id": "test_campaign",  # Use a specific test campaign ID
        "device": "desktop",
    }

    # Initialize producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"   📤 Sending test event (ID: {test_id})...")
    # Send event
    producer.send(KAFKA_TOPIC, value=test_event)
    producer.flush()
    producer.close()
    print("   ✅ Test event sent to Kafka")

    print("   ⏳ Waiting 70 seconds for Spark to process...")
    print("      (Spark has 1-min window + 5-sec trigger + processing time)")

    # Wait loop: Spark Structured Streaming windowing introduces latency.
    # If the window is 1 minute, we must wait at least that long for the
    # result to be finalized and written to Mongo.
    for i in range(70, 0, -10):
        print(f"      {i}s remaining...", end="\r")  # overwrite line for countdown effect
        time.sleep(10)
    print()

    # Check if the result appears in MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["adtech"]

    # Query the campaign_metrics collection for our "test_campaign"
    result = db["campaign_metrics"].find_one(
        {"campaign_id": "test_campaign"},
        sort=[("window_start", -1)]
    )

    if result:
        # Success: Data moved from Kafka -> Spark -> Mongo
        print("   ✅ END-TO-END TEST PASSED!")
        print(f"      Found test data in MongoDB:")
        print(f"      Window: {result.get('window_start')}")
        print(f"      Hits: {result.get('total_hits')}")
    else:
        # Failure: Data got lost somewhere in the middle
        print("   ⚠️  Test event not found in MongoDB")
        print("   Possible issues:")
        print("   - Spark Streaming not running")
        print("   - Spark checkpoint issues")
        print("   - MongoDB write permissions")

    client.close()

except Exception as e:
    # Catch-all for unexpected script errors
    print(f"   ❌ End-to-end test failed: {e}")
    import traceback

    traceback.print_exc()

# Print summary footer
print("\n" + "=" * 70)
print("DIAGNOSTIC SUMMARY")
print("=" * 70)

print("\n✅ = Working | ⚠️  = Warning | ❌ = Critical Issue")

print("\n📋 NEXT STEPS:")
print("   1. Fix any ❌ issues first")
print("   2. Address ⚠️  warnings")
print("   3. Once all green, try: python demo_bot_attack_REAL.py --test-only")
print("   4. Then run full attack: python demo_bot_attack_REAL.py --duration 30")

print("\n" + "=" * 70)